import threading
import time
from datetime import datetime
from common.config import HEARTBEAT_TIMEOUT, WORKER_STARTUP_GRACE
from common.models import WorkerStatus
from coordinator import database as db


class HeartbeatMonitor:
    def __init__(self, ring, reassigner):
        self.ring        = ring
        self.reassigner  = reassigner
        self._stop_event = threading.Event()

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
                self._handle_dead_worker(worker_id)
            else:
                print(f"[monitor] ✅ {worker_id} healthy ({seconds_since:.1f}s since last ping)")

    def _handle_dead_worker(self, worker_id: str):
        self.ring.remove_worker(worker_id)
        db.update_worker_status(worker_id, WorkerStatus.DEAD)
        self.reassigner.reassign_jobs_from(worker_id)
