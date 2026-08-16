"""FastAPI coordinator entrypoint.

POST /jobs is bounded and rate-limited (routing_executor, submission_limiter
— see issue #8) rather than fully async/queue-based, specifically to keep
the existing synchronous {job_id, status, worker_id} response contract that
chaos_test.py and tests/test_failure.py rely on.
"""

import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime

sys.path.insert(0, '.')

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager

from coordinator.hash_ring import ConsistentHashRing
from coordinator.job_router import JobRouter
from coordinator.heartbeat_monitor import HeartbeatMonitor
from coordinator.reassigner import Reassigner
from coordinator import database as db
from common.config import VIRTUAL_NODES, DASHBOARD_ORIGINS
# from coordinator import database as db

ring       = ConsistentHashRing(virtual_nodes=VIRTUAL_NODES)
router     = JobRouter(ring)
reassigner = Reassigner(ring, router)
monitor    = HeartbeatMonitor(ring, reassigner)

# POST /jobs used to call router.route() (a blocking gRPC call) directly on
# whatever thread FastAPI happened to hand the request — unbounded, no
# backpressure, so a burst of submissions could exhaust FastAPI's own
# thread pool with no signal to the client that the coordinator is
# overloaded. Routing now goes through this dedicated, bounded executor
# instead. See issue #8 — and the PR description for why this bounds
# concurrency + adds backpressure rather than fully decoupling submission
# from routing (that would drop worker_id from the synchronous response,
# which chaos_test.py and tests/test_failure.py both depend on today).
ROUTING_MAX_WORKERS      = 20
ROUTING_QUEUE_LIMIT      = 200
ROUTING_TIMEOUT_SECONDS  = 10
routing_executor = ThreadPoolExecutor(
    max_workers=ROUTING_MAX_WORKERS, thread_name_prefix="job-routing"
)


class SlidingWindowRateLimiter:
    """Simple in-memory rate limiter — no new dependency (e.g. slowapi)
    needed for a single-process coordinator. Tracks submission timestamps
    in a deque and evicts anything older than the window on each check.
    Not distributed-safe (per-process only), which is fine for the current
    single-coordinator setup; would need a shared store (Redis is already
    in this stack) if the coordinator is ever scaled horizontally."""

    def __init__(self, max_per_window: int, window_seconds: float):
        self.max_per_window = max_per_window
        self.window_seconds = window_seconds
        self._timestamps = deque()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        now = time.monotonic()
        with self._lock:
            while self._timestamps and now - self._timestamps[0] > self.window_seconds:
                self._timestamps.popleft()
            if len(self._timestamps) >= self.max_per_window:
                return False
            self._timestamps.append(now)
            return True


JOB_SUBMISSION_RATE_LIMIT = 200  # submissions per window
JOB_SUBMISSION_RATE_WINDOW_SECONDS = 1.0
submission_limiter = SlidingWindowRateLimiter(
    JOB_SUBMISSION_RATE_LIMIT, JOB_SUBMISSION_RATE_WINDOW_SECONDS
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.create_tables()
    db.reset_all_workers()
    print(f"[coordinator] started — waiting for workers to register")
    time.sleep(5)
    db.reset_worker_heartbeats()
    monitor.start()
    yield
    monitor.stop()
    routing_executor.shutdown(wait=False)
    db.close_pool()
    print("[coordinator] shut down")

app = FastAPI(title="Distributed Task Scheduler", lifespan=lifespan)

# The dashboard (dashboard/) is a separate origin (Vite dev server on :5173,
# or wherever it's deployed) making browser fetch() calls to this API on
# :8000 — without CORS headers those get blocked by the browser before the
# response even reaches the dashboard's JS. DASHBOARD_ORIGINS is env-backed
# (common/config.py) so a production dashboard deployment isn't stuck with
# the dev default.
app.add_middleware(
    CORSMiddleware,
    allow_origins=DASHBOARD_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

class JobSubmit(BaseModel):
    command: str

class WorkerRegister(BaseModel):
    worker_id: str
    address:   str
    port:      int

@app.post("/jobs")
def submit_job(body: JobSubmit):
    if not submission_limiter.allow():
        raise HTTPException(
            status_code=429,
            detail=f"rate limit exceeded ({JOB_SUBMISSION_RATE_LIMIT}/s) — try again shortly",
        )

    queue_depth = routing_executor._work_queue.qsize()
    if queue_depth > ROUTING_QUEUE_LIMIT:
        raise HTTPException(
            status_code=429,
            detail=f"coordinator overloaded (routing queue depth {queue_depth}) — try again shortly",
        )

    job_id = db.create_job(body.command)
    print(f"[coordinator] job created: {job_id}")

    future = routing_executor.submit(router.route, job_id, body.command)
    try:
        worker_id = future.result(timeout=ROUTING_TIMEOUT_SECONDS)
    except FutureTimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"routing job {job_id} timed out after {ROUTING_TIMEOUT_SECONDS}s",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return {"job_id": job_id, "status": "assigned", "worker_id": worker_id}

@app.get("/jobs")
def list_jobs(
    skip: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=2000),
    status: str | None = Query(None, description="filter by job status, e.g. 'failed'"),
    since: datetime | None = Query(None, description="only jobs created at or after this time"),
):
    # Default limit (200) is generous relative to this demo's scale so
    # existing manual usage (`curl :8000/jobs`) doesn't get silently
    # truncated — callers who want everything can still page through with
    # skip/limit, or filter down with status/since. See issue #10.
    return db.get_jobs_filtered(skip=skip, limit=limit, status=status, since=since)

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.post("/workers/register")
def register_worker(body: WorkerRegister):
    db.upsert_worker(body.worker_id, body.address, body.port)
    db.update_worker_heartbeat(body.worker_id)
    ring.add_worker(body.worker_id, {"address": body.address, "port": body.port})
    print(f"[coordinator] registered {body.worker_id} — ring now has: {ring.get_all_workers()}")
    return {"status": "registered", "worker_id": body.worker_id}

@app.post("/workers/{worker_id}/heartbeat")
def heartbeat(worker_id: str):
    db.update_worker_heartbeat(worker_id)
    return {"acknowledged": True}

@app.get("/workers")
def list_workers(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    return db.get_workers_paginated(skip=skip, limit=limit)

@app.get("/debug/ring")
def debug_ring():
    return {
        "workers_in_ring": ring.get_all_workers(),
        "workers_in_db": [w["worker_id"] for w in db.get_all_workers()],
        "ring_size": len(ring.ring)
    }
