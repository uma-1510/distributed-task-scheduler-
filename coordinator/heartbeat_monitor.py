import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from common.config import HEARTBEAT_TIMEOUT, WORKER_STARTUP_GRACE
from common.models import WorkerStatus
from coordinator import database as db


class HeartbeatMonitor:
    def __init__(self, ring, reassigner):
        self.ring        = ring
        self.reassigner  = reassigner
        self._stop_event = threading.Event()
        # Reassignment (which does its own DB writes + gRPC routing calls
        # per stuck job) used to run synchronously inside the monitor loop.
        # If one dead worker had a lot of stuck jobs, or a route() call hit
        # the new gRPC timeout from issue #5, that could delay the *next*
        # 5-second check cycle — meaning other workers' failures would be
        # detected late. Running it on a small executor instead decouples
        # "detect a dead worker" from "finish reassigning its jobs".
        self._reassign_executor = ThreadPoolExecutor(
            max_workers=5, thread_name_prefix="reassign"
        )

    def start(self):
        thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="heartbeat-monitor"
        )
        thread.start()
        print("[monitor] heartbeat monitor started")

    def stop(self):
        self._stop_event.set()
        self._reassign_executor.shutdown(wait=False)

    def _monitor_loop(self):
        while not self._stop_event.is_set():
            try:
                self._check_workers()
            except Exception as e:
                print(f"[monitor] error during check: {e}")
            time.sleep(5)

    @staticmethod
    def _strip_tz(dt):
        """Always return a naive datetime regardless of input."""
        if dt is None:
            return None
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            return dt.replace(tzinfo=None)
        return dt

    def _check_workers(self):
        workers = db.get_all_workers()
        now     = datetime.now()

        # Collect this cycle's dead workers instead of writing to the DB
        # inside the loop — one batch UPDATE at the end instead of one
        # round-trip per dead worker. See issue #6.
        newly_dead = []

        for worker in workers:
            worker_id      = worker["worker_id"]
            status         = worker["status"]
            last_heartbeat = self._strip_tz(worker["last_heartbeat"])
            registered_at  = self._strip_tz(worker["registered_at"])

            if status == WorkerStatus.DEAD.value:
                continue

            if last_heartbeat is None:
                continue

            seconds_since = (now - last_heartbeat).total_seconds()
            worker_age    = (now - registered_at).total_seconds() if registered_at else 999

            if worker_age < WORKER_STARTUP_GRACE:
                print(f"[monitor] ⏳ {worker_id} in grace period ({worker_age:.1f}s old)")
                continue

            if seconds_since > HEARTBEAT_TIMEOUT:
                print(f"[monitor] ⚠️  {worker_id} missed heartbeat ({seconds_since:.1f}s) — marking DEAD")
                newly_dead.append(worker_id)
            else:
                print(f"[monitor] ✅ {worker_id} healthy ({seconds_since:.1f}s since last ping)")

        if newly_dead:
            self._handle_dead_workers(newly_dead)

    def _handle_dead_workers(self, worker_ids: list[str]):
        for worker_id in worker_ids:
            self.ring.remove_worker(worker_id)

        # Single UPDATE for every worker that died this cycle, instead of
        # one per worker.
        db.update_workers_status_batch(worker_ids, WorkerStatus.DEAD)

        for worker_id in worker_ids:
            future = self._reassign_executor.submit(self._reassign_safely, worker_id)
            # Attach a callback rather than calling future.result() here —
            # that would just re-introduce the blocking we're trying to
            # remove.
            future.add_done_callback(self._log_reassign_outcome)

    def _reassign_safely(self, worker_id: str):
        self.reassigner.reassign_jobs_from(worker_id)
        return worker_id

    @staticmethod
    def _log_reassign_outcome(future):
        try:
            worker_id = future.result()
            print(f"[monitor] reassignment finished for {worker_id}")
        except Exception as e:
            print(f"[monitor] reassignment task raised: {e}")
