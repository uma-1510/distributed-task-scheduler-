# Routes jobs to workers via gRPC
#
# Known follow-up gap (see issue #5's PR discussion): a worker removed from
# the ring due to a timeout/UNAVAILABLE doesn't currently find its way back
# in on its own — it only re-registers at process startup. If it was a
# transient blip (e.g. a GC pause) rather than an actual crash, the worker
# stays excluded from routing until it's restarted. Worth a dedicated
# "re-add on next successful heartbeat" fix as a separate issue.
#
# Second known gap, partially mitigated (see route()'s except block, flagged
# in review): on DEADLINE_EXCEEDED specifically, the worker may have already
# received and started the job before the RPC timed out on our side. We
# still record the job as assigned to that worker so the reassignment path
# can recover it once the worker is confirmed dead — but AssignJob isn't
# idempotent, so if the worker really did get the job, it could run twice
# (once there, once wherever it's reassigned to). Full fix needs an
# idempotent AssignJob keyed by job_id plus a reconciliation step — a bigger
# change than this scoped mitigation; tracked as a follow-up, not done here.

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

        Worker address/port come from the ring's own metadata (set when the
        worker registers) rather than a DB query — see issue #4. That means
        this function does zero DB reads before the gRPC call; the only DB
        write is update_job_assigned() after a successful AssignJob.

        get_worker_and_metadata() does selection + metadata lookup as one
        atomic operation under the ring's own lock — using two separate
        calls (get_worker() then a plain dict .get()) had a race where
        another thread's remove_worker() could run in between, returning a
        worker_id whose metadata had just vanished. See hash_ring.py.
        """
        worker_id, worker = self.ring.get_worker_and_metadata(job_id)
        if not worker_id:
            raise RuntimeError("No healthy workers available in the ring")

        # Address/port live on the ring already (set in add_worker()), so
        # there's no need to hit the DB just to look up connection info for
        # a worker we already know the id of — see issue #4.
        if not worker:
            raise RuntimeError(f"Worker {worker_id} in ring but has no metadata")
        if "address" not in worker or "port" not in worker:
            # Shouldn't happen given add_worker()'s call sites always pass
            # both, but a malformed metadata dict here would otherwise show
            # up as a confusing KeyError deep in an f-string.
            raise RuntimeError(f"Worker {worker_id} metadata missing address/port: {worker}")

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

                # Scoped mitigation for a gap flagged in review (not the full
                # fix — see note below): without this, the job's DB row stays
                # 'pending' forever. Nothing anywhere re-scans pending jobs,
                # so it would be silently lost even once this worker is
                # confirmed DEAD — the reassigner only looks at jobs whose
                # assigned_to matches the dead worker and status is
                # 'assigned'/'running'. Recording it as assigned here (even
                # though the RPC didn't confirm the worker got it) means the
                # existing reassignment path picks it up once the heartbeat
                # monitor marks this worker dead, instead of it vanishing.
                # Best-effort: a DB failure here shouldn't mask the real error.
                try:
                    db.update_job_assigned(job_id, worker_id)
                except Exception as db_error:
                    print(f"[router]   also failed to record assignment for recovery: {db_error}")

                raise RuntimeError(f"Worker {worker_id} unreachable ({code.name})") from e
            raise RuntimeError(f"Worker {worker_id} gRPC call failed: {e.details()}") from e

        # Update job state in DB
        db.update_job_assigned(job_id, worker_id)
        print(f"[router] job {job_id} assigned to {worker_id} — status: {response.status}")
        return worker_id