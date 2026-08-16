from concurrent.futures import ThreadPoolExecutor, as_completed

from coordinator import database as db
from coordinator.hash_ring import ConsistentHashRing

# Routing N stuck jobs one at a time (each a blocking gRPC call, now up to
# 5s per issue #5's timeout) meant a dead worker holding 100 jobs could take
# minutes to fully reassign. Route up to this many in parallel instead.
MAX_PARALLEL_REASSIGNMENTS = 10


class Reassigner:
    def __init__(self, ring: ConsistentHashRing, router):
        self.ring   = ring
        self.router = router

    def reassign_jobs_from(self, dead_worker_id: str):
        # Find all jobs that were assigned/running on the dead worker
        stuck_jobs = db.get_jobs_for_worker(
            dead_worker_id,
            statuses=["assigned", "running"]
        )

        if not stuck_jobs:
            print(f"[reassigner] no stuck jobs from {dead_worker_id}")
            return

        print(f"[reassigner] reassigning {len(stuck_jobs)} jobs from {dead_worker_id}")

        # Reset every stuck job to pending in one UPDATE instead of one
        # round-trip per job — see issue #7.
        job_ids = [str(job["job_id"]) for job in stuck_jobs]
        db.mark_jobs_pending_batch(job_ids)

        if not self.ring.get_all_workers():
            print(f"[reassigner] no workers available — {len(job_ids)} job(s) stay pending")
            return

        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_REASSIGNMENTS) as executor:
            futures = {
                executor.submit(self.router.route, str(job["job_id"]), job["command"]): str(job["job_id"])
                for job in stuck_jobs
            }
            for future in as_completed(futures):
                job_id = futures[future]
                try:
                    new_worker = future.result()
                    print(f"[reassigner] ✅ job {job_id} → {new_worker}")
                except Exception as e:
                    # One job's routing failure (e.g. no workers left mid-
                    # batch, or it hit another dead worker) doesn't cancel
                    # the rest — each future is independent.
                    print(f"[reassigner] ❌ failed to reassign job {job_id}: {e}")