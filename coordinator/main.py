import sys
import time
sys.path.insert(0, '.')

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

from coordinator.hash_ring import ConsistentHashRing
from coordinator.job_router import JobRouter
from coordinator.heartbeat_monitor import HeartbeatMonitor
from coordinator.reassigner import Reassigner
from coordinator import database as db
from common.config import VIRTUAL_NODES
# from coordinator import database as db

ring       = ConsistentHashRing(virtual_nodes=VIRTUAL_NODES)
router     = JobRouter(ring)
reassigner = Reassigner(ring, router)
monitor    = HeartbeatMonitor(ring, reassigner)

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
    print("[coordinator] shut down")

app = FastAPI(title="Distributed Task Scheduler", lifespan=lifespan)

class JobSubmit(BaseModel):
    command: str

class WorkerRegister(BaseModel):
    worker_id: str
    address:   str
    port:      int

@app.post("/jobs")
def submit_job(body: JobSubmit):
    job_id = db.create_job(body.command)
    print(f"[coordinator] job created: {job_id}")
    try:
        worker_id = router.route(job_id, body.command)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"job_id": job_id, "status": "assigned", "worker_id": worker_id}

@app.get("/jobs")
def list_jobs():
    return db.get_all_jobs()

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
def list_workers():
    return db.get_all_workers()

@app.get("/debug/ring")
def debug_ring():
    return {
        "workers_in_ring": ring.get_all_workers(),
        "workers_in_db": [w["worker_id"] for w in db.get_all_workers()],
        "ring_size": len(ring.ring)
    }
