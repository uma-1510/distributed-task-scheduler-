# Routes jobs to workers via gRPC

import grpc
import sys
sys.path.insert(0, '.')

from proto import scheduler_pb2, scheduler_pb2_grpc
from coordinator.hash_ring import ConsistentHashRing
from coordinator import database as db

# How long we'll wait for a worker to acknowledge AssignJob before giving up
# on it. Without this, a slow or half-dead worker (marked healthy but not
# actually responding) can hang the gRPC call indefinitely, blocking whatever
# called route() — see issue #5.
ASSIGN_JOB_TIMEOUT_SECONDS = 5

# gRPC codes that mean "this worker isn't actually reachable right now" —
# worth pulling it out of the ring immediately rather than waiting for the
# heartbeat monitor's next cycle. DEADLINE_EXCEEDED is a hung call;
# UNAVAILABLE is an immediate connection-refused, e.g. right after
# `docker compose stop <worker>` closes the port before the container is
# fully gone.
UNREACHABLE_WORKER_CODES = {grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE}


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
        try:
            with grpc.insecure_channel(address) as channel:
                stub = scheduler_pb2_grpc.WorkerServiceStub(channel)
                response = stub.AssignJob(
                    scheduler_pb2.JobRequest(job_id=job_id, command=command),
                    timeout=ASSIGN_JOB_TIMEOUT_SECONDS,
                )
        except grpc.RpcError as e:
            code = e.code()
            if code in UNREACHABLE_WORKER_CODES:
                # This worker isn't responding even though the heartbeat
                # monitor hasn't marked it DEAD yet (still within its grace
                # window). Pull it out of the ring immediately so the next
                # route() call doesn't pick it again — the heartbeat monitor
                # will catch up and mark it DEAD/reassign its other jobs on
                # its own schedule.
                print(f"[router] ⏱️  {worker_id} unreachable ({code.name}) — removing from ring")
                self.ring.remove_worker(worker_id)
                raise RuntimeError(f"Worker {worker_id} unreachable ({code.name})") from e
            raise RuntimeError(f"Worker {worker_id} gRPC call failed: {e.details()}") from e

        # Update job state in DB
        db.update_job_assigned(job_id, worker_id)
        print(f"[router] job {job_id} assigned to {worker_id} — status: {response.status}")
        return worker_id