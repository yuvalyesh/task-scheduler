"""Tests for the task scheduler core."""

import time
import pytest
from scheduler.core import Scheduler, Task, TaskStatus


@pytest.fixture
def scheduler():
    sched = Scheduler(workers=2)
    yield sched
    sched.shutdown(wait=True)


def test_simple_task_succeeds(scheduler):
    task = Task.create(lambda: 42, priority=1)
    tid = scheduler.submit(task)
    result = scheduler.result(tid, timeout=3)
    assert result == 42
    assert scheduler.status(tid) == TaskStatus.SUCCESS


def test_priority_ordering(scheduler):
    """Lower priority number should generally execute first."""
    order = []
    for p in [5, 1, 3]:
        t = Task.create(lambda p=p: order.append(p), priority=p)
        scheduler.submit(t)
    time.sleep(0.5)
    assert order[0] == 1  # highest priority ran first


def test_dependency_resolution(scheduler):
    """Task B must not start before task A finishes."""
    timeline = []
    a = Task.create(lambda: timeline.append("A"), priority=1, task_id="task-a")
    b = Task.create(lambda: timeline.append("B"), priority=1, dependencies=["task-a"], task_id="task-b")
    scheduler.submit(a)
    scheduler.submit(b)
    time.sleep(0.5)
    assert timeline == ["A", "B"]


def test_retry_on_failure(scheduler):
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("not yet")
        return "ok"

    task = Task.create(flaky, priority=1, max_retries=3, retry_delay=0.05)
    tid = scheduler.submit(task)
    result = scheduler.result(tid, timeout=5)
    assert result == "ok"
    assert len(attempts) == 3


def test_cancel_pending_task(scheduler):
    # Saturate workers so our task stays pending
    blockers = [Task.create(lambda: time.sleep(1), priority=1) for _ in range(2)]
    for b in blockers:
        scheduler.submit(b)

    target = Task.create(lambda: None, priority=5)
    tid = scheduler.submit(target)
    time.sleep(0.05)
    cancelled = scheduler.cancel(tid)
    assert cancelled is True
    assert scheduler.status(tid) == TaskStatus.CANCELLED


def test_stats(scheduler):
    task = Task.create(lambda: time.sleep(0.1), priority=1)
    scheduler.submit(task)
    time.sleep(0.3)
    stats = scheduler.stats()
    assert stats["SUCCESS"] >= 1


def test_duplicate_task_id_raises(scheduler):
    t1 = Task.create(lambda: None, priority=1, task_id="dup")
    t2 = Task.create(lambda: None, priority=1, task_id="dup")
    scheduler.submit(t1)
    with pytest.raises(ValueError, match="Duplicate task_id"):
        scheduler.submit(t2)
