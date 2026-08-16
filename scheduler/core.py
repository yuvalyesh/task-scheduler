"""
Core task scheduler: thread-pool backed, priority-queue ordered,
with dependency resolution and retry logic.
"""

from __future__ import annotations

import heapq
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass(order=True)
class Task:
    """A unit of schedulable work."""
    priority: int
    task_id: str = field(compare=False)
    fn: Callable = field(compare=False)
    args: tuple = field(default_factory=tuple, compare=False)
    kwargs: dict = field(default_factory=dict, compare=False)
    dependencies: Set[str] = field(default_factory=set, compare=False)
    max_retries: int = field(default=0, compare=False)
    retry_delay: float = field(default=1.0, compare=False)

    # Runtime state (not part of ordering)
    status: TaskStatus = field(default=TaskStatus.PENDING, compare=False)
    result: Optional[object] = field(default=None, compare=False)
    error: Optional[Exception] = field(default=None, compare=False)
    attempt: int = field(default=0, compare=False)
    submitted_at: float = field(default_factory=time.monotonic, compare=False)
    completed_at: Optional[float] = field(default=None, compare=False)
    _done_event: threading.Event = field(default_factory=threading.Event, compare=False)

    @staticmethod
    def create(
        fn: Callable,
        *args,
        priority: int = 5,
        dependencies: Optional[List[str]] = None,
        max_retries: int = 0,
        retry_delay: float = 1.0,
        task_id: Optional[str] = None,
        **kwargs,
    ) -> "Task":
        return Task(
            priority=priority,
            task_id=task_id or str(uuid.uuid4()),
            fn=fn,
            args=args,
            kwargs=kwargs,
            dependencies=set(dependencies or []),
            max_retries=max_retries,
            retry_delay=retry_delay,
        )


class Scheduler:
    """
    Priority-queue task scheduler backed by a thread pool.

    Tasks are dispatched in priority order (lower value = higher priority).
    A task with unresolved dependencies is held until all deps complete
    successfully. Failed tasks are retried up to `max_retries` times with
    an exponential backoff.
    """

    def __init__(self, workers: int = 4):
        self._lock = threading.Lock()
        self._heap: List[Task] = []       # min-heap by priority
        self._tasks: Dict[str, Task] = {} # task_id → Task
        self._completed: Set[str] = set() # successfully finished task IDs
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sched-worker")
        self._shutdown = threading.Event()
        self._dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True, name="sched-dispatcher")
        self._dispatcher.start()
        logger.info("Scheduler started with %d workers", workers)

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def submit(self, task: Task) -> str:
        """Enqueue a task. Returns its task_id."""
        with self._lock:
            if task.task_id in self._tasks:
                raise ValueError(f"Duplicate task_id: {task.task_id}")
            self._tasks[task.task_id] = task
            heapq.heappush(self._heap, task)
            logger.debug("Submitted task %s (priority=%d)", task.task_id, task.priority)
        return task.task_id

    def cancel(self, task_id: str) -> bool:
        """Cancel a pending task. Returns True if cancelled, False if already running/done."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task and task.status == TaskStatus.PENDING:
                task.status = TaskStatus.CANCELLED
                task._done_event.set()
                logger.info("Cancelled task %s", task_id)
                return True
        return False

    def status(self, task_id: str) -> Optional[TaskStatus]:
        task = self._tasks.get(task_id)
        return task.status if task else None

    def result(self, task_id: str, timeout: Optional[float] = None) -> object:
        """Block until the task completes and return its result (or raise its exception)."""
        task = self._tasks.get(task_id)
        if not task:
            raise KeyError(f"Unknown task: {task_id}")
            
        if not task._done_event.wait(timeout=timeout):
            raise TimeoutError(f"Task {task_id} did not complete within timeout")
            
        if task.status == TaskStatus.SUCCESS:
            return task.result
        elif task.status == TaskStatus.FAILED:
            raise task.error
        elif task.status == TaskStatus.CANCELLED:
            raise RuntimeError(f"Task {task_id} was cancelled")
        else:
            raise RuntimeError(f"Unexpected task status: {task.status}")

    def shutdown(self, wait: bool = True) -> None:
        self._shutdown.set()
        if wait:
            self._dispatcher.join(timeout=5)
        self._executor.shutdown(wait=wait)
        logger.info("Scheduler shut down")

    def stats(self) -> Dict[str, int]:
        with self._lock:
            counts: Dict[str, int] = {s.name: 0 for s in TaskStatus}
            for t in self._tasks.values():
                counts[t.status.name] += 1
            return counts

    # ------------------------------------------------------------------ #
    # Internal dispatch loop                                               #
    # ------------------------------------------------------------------ #

    def _dispatch_loop(self) -> None:
        while not self._shutdown.is_set():
            with self._lock:
                ready = self._pop_ready()
            for task in ready:
                self._launch(task)
            time.sleep(0.05)  # 50 ms poll interval

    def _pop_ready(self) -> List[Task]:
        """Extract all dispatchable tasks from the heap (deps met, not cancelled)."""
        ready: List[Task] = []
        remaining: List[Task] = []

        while self._heap:
            task = heapq.heappop(self._heap)
            if task.status == TaskStatus.CANCELLED:
                continue
            if task.status != TaskStatus.PENDING:
                continue
            if task.dependencies <= self._completed:
                ready.append(task)
            else:
                remaining.append(task)

        for t in remaining:
            heapq.heappush(self._heap, t)

        return ready

    def _launch(self, task: Task) -> None:
        task.status = TaskStatus.RUNNING
        task.attempt += 1
        self._executor.submit(self._run_task, task)

    def _run_task(self, task: Task) -> object:
        try:
            result = task.fn(*task.args, **task.kwargs)
            task.result = result
            task.status = TaskStatus.SUCCESS
            task.completed_at = time.monotonic()
            with self._lock:
                self._completed.add(task.task_id)
            task._done_event.set()
            logger.info("Task %s succeeded (attempt %d)", task.task_id, task.attempt)
            return result
        except Exception as exc:
            task.error = exc
            logger.warning("Task %s failed (attempt %d): %s", task.task_id, task.attempt, exc)
            if task.attempt <= task.max_retries:
                delay = task.retry_delay * (2 ** (task.attempt - 1))
                time.sleep(delay)
                task.status = TaskStatus.PENDING
                with self._lock:
                    heapq.heappush(self._heap, task)
            else:
                task.status = TaskStatus.FAILED
                task.completed_at = time.monotonic()
                task._done_event.set()
                raise
