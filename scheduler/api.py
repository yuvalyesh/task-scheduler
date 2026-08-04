"""
FastAPI REST interface for the task scheduler.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from scheduler.core import Scheduler, Task, TaskStatus

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Global scheduler instance                                           #
# ------------------------------------------------------------------ #

_scheduler: Optional[Scheduler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _scheduler = Scheduler(workers=4)
    logger.info("Scheduler initialised")
    yield
    _scheduler.shutdown(wait=True)
    logger.info("Scheduler shut down")


app = FastAPI(
    title="Task Scheduler API",
    description="Priority-queue task scheduler with dependency resolution and retry logic.",
    version="1.0.0",
    lifespan=lifespan,
)


# ------------------------------------------------------------------ #
# Pydantic schemas                                                    #
# ------------------------------------------------------------------ #

class TaskSubmitRequest(BaseModel):
    name: str = Field(..., description="Human-readable label (used as fn lookup key)")
    priority: int = Field(5, ge=1, le=10, description="1 = highest, 10 = lowest")
    dependencies: List[str] = Field(default_factory=list, description="task_ids that must succeed first")
    max_retries: int = Field(0, ge=0, le=5)
    retry_delay: float = Field(1.0, ge=0.1)
    payload: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary task arguments")


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    priority: int
    attempt: int
    error: Optional[str] = None


class StatsResponse(BaseModel):
    PENDING: int
    RUNNING: int
    SUCCESS: int
    FAILED: int
    CANCELLED: int


# ------------------------------------------------------------------ #
# Task function registry                                              #
# ------------------------------------------------------------------ #

# In a real system this would be a plugin registry or Celery task map.
# Here we include a few demo tasks for illustration.

import time as _time


def _demo_echo(payload: dict) -> dict:
    _time.sleep(0.1)
    return {"echo": payload}


def _demo_slow(payload: dict) -> dict:
    _time.sleep(float(payload.get("duration", 2)))
    return {"done": True}


def _demo_failing(payload: dict) -> dict:
    raise RuntimeError("Intentional failure for retry demo")


TASK_REGISTRY: Dict[str, Any] = {
    "echo": _demo_echo,
    "slow": _demo_slow,
    "failing": _demo_failing,
}


# ------------------------------------------------------------------ #
# Routes                                                              #
# ------------------------------------------------------------------ #

@app.post("/tasks", response_model=TaskStatusResponse, status_code=201)
def submit_task(req: TaskSubmitRequest):
    """Submit a new task to the scheduler."""
    fn = TASK_REGISTRY.get(req.name)
    if fn is None:
        raise HTTPException(status_code=400, detail=f"Unknown task name '{req.name}'. "
                            f"Available: {list(TASK_REGISTRY)}")
    task = Task.create(
        fn,
        req.payload,
        priority=req.priority,
        dependencies=req.dependencies,
        max_retries=req.max_retries,
        retry_delay=req.retry_delay,
    )
    _scheduler.submit(task)
    return TaskStatusResponse(
        task_id=task.task_id,
        status=task.status.name,
        priority=task.priority,
        attempt=task.attempt,
    )


@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task(task_id: str):
    """Get the current status of a task."""
    status = _scheduler.status(task_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Task not found")
    task = _scheduler._tasks[task_id]
    return TaskStatusResponse(
        task_id=task_id,
        status=status.name,
        priority=task.priority,
        attempt=task.attempt,
        error=str(task.error) if task.error else None,
    )


@app.delete("/tasks/{task_id}")
def cancel_task(task_id: str):
    """Cancel a pending task."""
    cancelled = _scheduler.cancel(task_id)
    if not cancelled:
        raise HTTPException(status_code=409, detail="Task not pending or not found")
    return {"cancelled": task_id}


@app.get("/stats", response_model=StatsResponse)
def get_stats():
    """Return task count by status."""
    return _scheduler.stats()


@app.get("/health")
def health():
    return {"status": "ok"}
