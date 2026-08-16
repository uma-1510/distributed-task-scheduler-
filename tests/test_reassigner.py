# Unit tests for coordinator/reassigner.py's batched pending-marks and
# parallel routing (issue #7). Mocks the DB and router so these run without
# a live cluster.

import sys
from unittest.mock import MagicMock, call, patch

sys.path.insert(0, '.')

from coordinator.reassigner import Reassigner


def _stuck_jobs(n):
    return [{"job_id": f"job-{i}", "command": f"echo {i}"} for i in range(n)]


def test_no_stuck_jobs_is_a_noop():
    print("\n--- TEST 1: no stuck jobs -> nothing marked pending, no routing ---")
    ring = MagicMock()
    router = MagicMock()
    reassigner = Reassigner(ring, router)

    with patch("coordinator.reassigner.db.get_jobs_for_worker", return_value=[]), \
         patch("coordinator.reassigner.db.mark_jobs_pending_batch") as mock_batch:
        reassigner.reassign_jobs_from("worker-1")

    mock_batch.assert_not_called()
    router.route.assert_not_called()
    print("PASSED")


def test_stuck_jobs_marked_pending_in_a_single_batch_call():
    print("\n--- TEST 2: N stuck jobs -> one mark_jobs_pending_batch call with all N ids ---")
    ring = MagicMock()
    ring.get_all_workers.return_value = ["worker-2"]
    router = MagicMock()
    router.route.return_value = "worker-2"
    reassigner = Reassigner(ring, router)

    jobs = _stuck_jobs(5)

    with patch("coordinator.reassigner.db.get_jobs_for_worker", return_value=jobs), \
         patch("coordinator.reassigner.db.mark_jobs_pending_batch") as mock_batch:
        reassigner.reassign_jobs_from("worker-1")

    mock_batch.assert_called_once()
    assert set(mock_batch.call_args[0][0]) == {f"job-{i}" for i in range(5)}
    print("PASSED")


def test_no_workers_available_skips_routing_entirely():
    print("\n--- TEST 3: empty ring -> jobs marked pending but router.route never called ---")
    ring = MagicMock()
    ring.get_all_workers.return_value = []  # nothing left to route to
    router = MagicMock()
    reassigner = Reassigner(ring, router)

    jobs = _stuck_jobs(3)

    with patch("coordinator.reassigner.db.get_jobs_for_worker", return_value=jobs), \
         patch("coordinator.reassigner.db.mark_jobs_pending_batch") as mock_batch:
        reassigner.reassign_jobs_from("worker-1")

    mock_batch.assert_called_once()
    router.route.assert_not_called()
    print("PASSED")


def test_all_jobs_get_routed_when_workers_available():
    print("\n--- TEST 4: every stuck job gets routed exactly once ---")
    ring = MagicMock()
    ring.get_all_workers.return_value = ["worker-2", "worker-3"]
    router = MagicMock()
    router.route.return_value = "worker-2"
    reassigner = Reassigner(ring, router)

    jobs = _stuck_jobs(8)

    with patch("coordinator.reassigner.db.get_jobs_for_worker", return_value=jobs), \
         patch("coordinator.reassigner.db.mark_jobs_pending_batch"):
        reassigner.reassign_jobs_from("worker-1")

    assert router.route.call_count == 8
    routed_job_ids = {c.args[0] for c in router.route.call_args_list}
    assert routed_job_ids == {f"job-{i}" for i in range(8)}
    print("PASSED")


def test_one_job_failing_to_route_does_not_block_the_others():
    print("\n--- TEST 5: one job's routing failure doesn't stop the rest from being attempted ---")
    ring = MagicMock()
    ring.get_all_workers.return_value = ["worker-2"]
    router = MagicMock()

    def flaky_route(job_id, command):
        if job_id == "job-2":
            raise RuntimeError("simulated routing failure")
        return "worker-2"

    router.route.side_effect = flaky_route
    reassigner = Reassigner(ring, router)

    jobs = _stuck_jobs(5)

    with patch("coordinator.reassigner.db.get_jobs_for_worker", return_value=jobs), \
         patch("coordinator.reassigner.db.mark_jobs_pending_batch"):
        # Should not raise even though one job's route() call raises.
        reassigner.reassign_jobs_from("worker-1")

    # All 5 were still attempted despite job-2 failing.
    assert router.route.call_count == 5
    print("PASSED")


if __name__ == "__main__":
    test_no_stuck_jobs_is_a_noop()
    test_stuck_jobs_marked_pending_in_a_single_batch_call()
    test_no_workers_available_skips_routing_entirely()
    test_all_jobs_get_routed_when_workers_available()
    test_one_job_failing_to_route_does_not_block_the_others()
    print("\n✅ All tests passed — reassigner batching/parallel routing is working correctly")
