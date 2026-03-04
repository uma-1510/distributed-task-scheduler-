# FastAPI app, REST endpoints
import sys
sys.path.insert(0, '.')

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

from coordinator.hash_ring import ConsistentHashRing
from coordinator.job_router import JobRouter
from coordinator import database as db
from common.config import VIRTUAL_NODES


# Startup / shutdown

ring   = ConsistentHashRing(virtual_nodes=VIRTUAL_NODES)
router = JobRouter(ring)

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.create_tables()
    # Rebuild ring from any healthy workers already in DB
    for worker in db.get_healthy_workers():
        ring.add_worker(worker["worker_id"], {
            "address": worker["address"],
            "port":    worker["port"]
        })
    print(f"[coordinator] started — {len(ring.get_all_workers())} workers in ring")
    yield
    print("[coordinator] shutting down")

app = FastAPI(title="Distributed Task Scheduler", lifespan=lifespan)


# Request / response models

class JobSubmit(BaseModel):
    command: str

class WorkerRegister(BaseModel):
    worker_id: str
    address:   str
    port:      int


# Job endpoints

@app.post("/jobs")
def submit_job(body: JobSubmit):
    # 1. Create job record in DB with status=pending
    job_id = db.create_job(body.command)
    print(f"[coordinator] job created: {job_id}")

    # 2. Route to a worker via hash ring
    try:
        worker_id = router.route(job_id, body.command)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return {
        "job_id":    job_id,
        "status":    "assigned",
        "worker_id": worker_id
    }


@app.get("/jobs")
def list_jobs():
    return db.get_all_jobs()


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# Worker endpoints

@app.post("/workers/register")
def register_worker(body: WorkerRegister):
    db.upsert_worker(body.worker_id, body.address, body.port)
    ring.add_worker(body.worker_id, {
        "address": body.address,
        "port":    body.port
    })
    print(f"[coordinator] registered {body.worker_id} at {body.address}:{body.port}")
    return {"status": "registered", "worker_id": body.worker_id}


@app.post("/workers/{worker_id}/heartbeat")
def heartbeat(worker_id: str):
    db.update_worker_heartbeat(worker_id)
    return {"acknowledged": True}


@app.get("/workers")
def list_workers():
    return db.get_all_workers()