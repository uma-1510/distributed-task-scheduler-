# Unit tests for coordinator/heartbeat_monitor.py's batched dead-worker
# updates and off-thread reassignment (issue #6). Mocks the DB and
# reassigner so these run without a live cluster.

import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, '.')

from coordinator.heartbeat_monitor import HeartbeatMonitor


def _worker(worker_id, seconds_since_heartbeat, registered_seconds_ago=999):
    now = datetime.now()
    return {
        "worker_id": worker_id,
        "status": "healthy",
        "last_heartbeat": now - timedelta(seconds=seconds_since_heartbeat),
        "registered_at": now - timedelta(seconds=registered_seconds_ago),
    }


def test_check_workers_batches_dead_worker_updates():
    print("\n--- TEST 1: multiple dead workers in one cycle -> a single batch update call ---")
    ring = MagicMock()
    reassigner = MagicMock()
    monitor = HeartbeatMonitor(ring, reassigner)

    workers = [
        _worker("worker-1", seconds_since_heartbeat=20),  # dead (> 15s HEARTBEAT_TIMEOUT)
        _worker("worker-2", seconds_since_heartbeat=20),  # dead
        _worker("worker-3", seconds_since_heartbeat=2),   # healthy
    ]

    with patch("coordinator.heartbeat_monitor.db.get_all_workers", return_value=workers), \
         patch("coordinator.heartbeat_monitor.db.update_worker_status") as mock_single, \
         patch("coordinator.heartbeat_monitor.db.update_workers_status_batch") as mock_batch:
        monitor._check_workers()

    mock_single.assert_not_called()
    mock_batch.assert_called_once()
    called_ids = set(mock_batch.call_args[0][0])
    assert called_ids == {"worker-1", "worker-2"}

    monitor._reassign_executor.shutdown(wait=True)
    print("PASSED")


def test_check_workers_no_dead_workers_skips_batch_call():
    print("\n--- TEST 2: no dead workers -> batch update not called at all ---")
    ring = MagicMock()
    reassigner = MagicMock()
    monitor = HeartbeatMonitor(ring, reassigner)

    workers = [_worker("worker-1", seconds_since_heartbeat=2)]

    with patch("coordinator.heartbeat_monitor.db.get_all_workers", return_value=workers), \
         patch("coordinator.heartbeat_monitor.db.update_workers_status_batch") as mock_batch:
        monitor._check_workers()

    mock_batch.assert_not_called()
    monitor._reassign_executor.shutdown(wait=True)
    print("PASSED")


def test_dead_workers_removed_from_ring_and_reassigned_off_thread():
    print("\n--- TEST 3: dead worker removed from ring, reassignment runs (off the monitor thread) ---")
    ring = MagicMock()
    reassigner = MagicMock()
    monitor = HeartbeatMonitor(ring, reassigner)

    workers = [_worker("worker-1", seconds_since_heartbeat=20)]

    with patch("coordinator.heartbeat_monitor.db.get_all_workers", return_value=workers), \
         patch("coordinator.heartbeat_monitor.db.update_workers_status_batch"):
        monitor._check_workers()

    # Ring removal happens synchronously, before reassignment is even queued.
    ring.remove_worker.assert_called_once_with("worker-1")

    # Reassignment itself runs on the background executor — wait for it to
    # drain instead of asserting immediately (would be a race otherwise).
    monitor._reassign_executor.shutdown(wait=True)
    reassigner.reassign_jobs_from.assert_called_once_with("worker-1")
    print("PASSED")


if __name__ == "__main__":
    test_check_workers_batches_dead_worker_updates()
    test_check_workers_no_dead_workers_skips_batch_call()
    test_dead_workers_removed_from_ring_and_reassigned_off_thread()
    print("\n✅ All tests passed — heartbeat monitor batching/async reassignment is working correctly")
