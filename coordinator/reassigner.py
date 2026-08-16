from coordinator import database as db
from coordinator.hash_ring import ConsistentHashRing


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

        for job in stuck_jobs:
            job_id  = str(job["job_id"])
            command = job["command"]

            try:
                new_worker = self.router.route(job_id, command)
                print(f"[reassigner] ✅ job {job_id} → {new_worker}")
            except Exception as e:
                print(f"[reassigner] ❌ failed to reassign job {job_id}: {e}")