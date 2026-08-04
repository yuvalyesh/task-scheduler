# Task Scheduler

A concurrent, priority-queue–based task scheduler with a FastAPI REST interface. Supports task dependencies, automatic retry with exponential backoff, and pluggable worker pool sizing.

## Overview

```
POST /tasks  →  Scheduler  →  ThreadPoolExecutor
                    │              │
              priority heap   worker-1
              dep resolver    worker-2
              retry logic     worker-N
                    │
              GET /tasks/{id}  →  status / result
              GET /stats       →  counts by status
```

Tasks are dispatched in priority order (1 = highest). A task with declared dependencies is held in the heap until all its upstream tasks complete successfully. On failure, tasks retry up to `max_retries` times with exponential backoff (`delay × 2^attempt`).

## Key Features

- **Priority scheduling** — Python `heapq` min-heap; lower priority value dispatched first.
- **Dependency resolution** — DAG-style: tasks declare upstream `task_id`s; dispatcher only pops tasks whose full dependency set is in the completed set.
- **Retry with exponential backoff** — Configurable `max_retries` and `retry_delay`; failed tasks are re-queued automatically.
- **Thread-safe state** — Single `threading.Lock` guards heap and task map; `ThreadPoolExecutor` manages worker lifecycle.
- **REST API** — FastAPI with async lifespan management; Pydantic v2 request/response schemas.
- **Test suite** — 7 pytest cases covering success, priority ordering, dependency sequencing, retry, cancel, stats, and duplicate ID detection.

## Tech Stack

| | |
|---|---|
| Language | Python 3.11+ |
| Framework | FastAPI + Uvicorn |
| Concurrency | `threading.ThreadPoolExecutor` |
| Validation | Pydantic v2 |
| Tests | pytest |
| Build | Makefile |

## Project Structure

```
task-scheduler/
├── scheduler/
│   ├── __init__.py
│   ├── core.py     # Scheduler, Task, TaskStatus — pure Python, no framework deps
│   └── api.py      # FastAPI app, routes, task registry
├── tests/
│   └── test_scheduler.py
├── requirements.txt
└── Makefile
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the API server
make run
# or: uvicorn scheduler.api:app --reload --port 8000

# Run tests
make test
```

API docs available at `http://localhost:8000/docs` (Swagger UI).

## API Reference

### Submit a task
```http
POST /tasks
Content-Type: application/json

{
  "name": "echo",
  "priority": 2,
  "dependencies": [],
  "max_retries": 2,
  "retry_delay": 0.5,
  "payload": {"message": "hello"}
}
```
Response:
```json
{"task_id": "f47ac10b-...", "status": "PENDING", "priority": 2, "attempt": 0}
```

### Check task status
```http
GET /tasks/f47ac10b-...
```

### Cancel a pending task
```http
DELETE /tasks/f47ac10b-...
```

### Scheduler stats
```http
GET /stats
```
```json
{"PENDING": 3, "RUNNING": 1, "SUCCESS": 12, "FAILED": 0, "CANCELLED": 1}
```

## Design Notes

- **`core.py` has zero framework dependencies** — the `Scheduler` and `Task` classes work standalone in any Python environment. The FastAPI layer in `api.py` is a thin adapter.
- **Dispatcher thread** runs a 50 ms poll loop, checking for newly-dispatchable tasks. This avoids complex condition-variable signalling while keeping latency low for most use cases.
- **Task registry** in `api.py` maps string names to callables. In production, this extends naturally to a plugin system or Celery-style task decorator.

## Concepts Demonstrated

- **Concurrent programming** — Thread pool, lock-based shared state, producer-consumer dispatch loop.
- **Data structures** — Min-heap for O(log n) priority dispatch; hash map for O(1) task lookup.
- **REST API design** — Resource-oriented endpoints, Pydantic validation, proper HTTP status codes.
- **Software engineering** — Separation of concerns (core vs. API layer), dependency injection via lifespan context, comprehensive test coverage.
