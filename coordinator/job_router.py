# Routes jobs to workers via gRPC

import grpc
import sys
sys.path.insert(0, '.')

from proto import scheduler_pb2, scheduler_pb2_grpc
from coordinator.hash_ring import ConsistentHashRing
from coordinator import database as db


class JobRouter:
    def __init__(self, ring: ConsistentHashRing):
        self.ring = ring

    def route(self, job_id: str, command: str) -> str:
        """
        Routes job to a worker via consistent hashing.
        Returns the worker_id it was sent to.
        Raises if no workers available or gRPC call fails.
        """
        worker_id = self.ring.get_worker(job_id)
        if not worker_id:
            raise RuntimeError("No healthy workers available in the ring")

        # Look up this worker's address and port from DB
        workers = db.get_all_workers()
        worker = next((w for w in workers if w["worker_id"] == worker_id), None)
        if not worker:
            raise RuntimeError(f"Worker {worker_id} in ring but not found in DB")

        address = f"{worker['address']}:{worker['port']}"
        print(f"[router] routing job {job_id} → {worker_id} at {address}")

        # Open gRPC channel and assign job
        with grpc.insecure_channel(address) as channel:
            stub = scheduler_pb2_grpc.WorkerServiceStub(channel)
            response = stub.AssignJob(
                scheduler_pb2.JobRequest(job_id=job_id, command=command)
            )

        # Update job state in DB
        db.update_job_assigned(job_id, worker_id)
        print(f"[router] job {job_id} assigned to {worker_id} — status: {response.status}")
        return worker_id