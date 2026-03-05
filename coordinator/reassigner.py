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

        for job in stuck_jobs:
            job_id  = str(job["job_id"])
            command = job["command"]

            # Check if any workers left in ring
            if not self.ring.get_all_workers():
                print(f"[reassigner] no workers available — job {job_id} stays pending")
                db.mark_job_pending(job_id)
                continue

            # Reset job to pending so it can be reassigned
            db.mark_job_pending(job_id)

            try:
                new_worker = self.router.route(job_id, command)
                print(f"[reassigner] ✅ job {job_id} → {new_worker}")
            except Exception as e:
                print(f"[reassigner] ❌ failed to reassign job {job_id}: {e}")