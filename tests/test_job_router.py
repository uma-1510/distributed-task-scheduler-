# Unit tests for coordinator/job_router.py's gRPC timeout/unreachable
# handling (issue #5). Mocks the gRPC stub so these run without a live
# worker process — the actual end-to-end path is exercised manually /
# against docker compose (see PR description), not here.

import sys
from unittest.mock import MagicMock, patch

import grpc

sys.path.insert(0, '.')

from coordinator.hash_ring import ConsistentHashRing
from coordinator.job_router import JobRouter


def _rpc_error(code, details="simulated failure"):
    class _FakeRpcError(grpc.RpcError):
        def code(self):
            return code

        def details(self):
            return details

    return _FakeRpcError()


def _router_with_one_worker():
    ring = ConsistentHashRing()
    ring.add_worker("worker-1", {"address": "worker-1", "port": 50051})
    return JobRouter(ring), ring


def test_deadline_exceeded_removes_worker_from_ring():
    print("\n--- TEST 1: DEADLINE_EXCEEDED removes the worker from the ring ---")
    router, ring = _router_with_one_worker()

    mock_stub = MagicMock()
    mock_stub.AssignJob.side_effect = _rpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

    with patch("coordinator.job_router.db.get_all_workers",
               return_value=[{"worker_id": "worker-1", "address": "worker-1", "port": 50051}]), \
         patch("coordinator.job_router.db.update_job_assigned") as mock_update, \
         patch("coordinator.job_router.scheduler_pb2_grpc.WorkerServiceStub", return_value=mock_stub), \
         patch("coordinator.job_router.grpc.insecure_channel") as mock_channel:
        mock_channel.return_value.__enter__.return_value = MagicMock()

        try:
            router.route("job-1", "echo hi")
            raise AssertionError("expected RuntimeError to be raised")
        except RuntimeError as e:
            assert "unreachable" in str(e).lower()

    assert "worker-1" not in ring.get_all_workers(), "worker should be removed from the ring after timeout"
    # See job_router.py's module docstring: recording the assignment here
    # (even without RPC confirmation) is what lets the reassigner recover
    # this job once the worker is confirmed dead, instead of it staying
    # 'pending' forever with no retry path.
    mock_update.assert_called_once_with("job-1", "worker-1")
    print("PASSED")


def test_unavailable_removes_worker_from_ring():
    print("\n--- TEST 2: UNAVAILABLE removes the worker from the ring ---")
    router, ring = _router_with_one_worker()

    mock_stub = MagicMock()
    mock_stub.AssignJob.side_effect = _rpc_error(grpc.StatusCode.UNAVAILABLE)

    with patch("coordinator.job_router.db.get_all_workers",
               return_value=[{"worker_id": "worker-1", "address": "worker-1", "port": 50051}]), \
         patch("coordinator.job_router.db.update_job_assigned") as mock_update, \
         patch("coordinator.job_router.scheduler_pb2_grpc.WorkerServiceStub", return_value=mock_stub), \
         patch("coordinator.job_router.grpc.insecure_channel") as mock_channel:
        mock_channel.return_value.__enter__.return_value = MagicMock()

        try:
            router.route("job-1", "echo hi")
            raise AssertionError("expected RuntimeError to be raised")
        except RuntimeError:
            pass

    assert "worker-1" not in ring.get_all_workers()
    mock_update.assert_called_once_with("job-1", "worker-1")
    print("PASSED")


def test_recovery_record_failure_does_not_mask_original_error():
    print("\n--- TEST 5: a DB failure while recording the recovery assignment doesn't hide the real error ---")
    router, ring = _router_with_one_worker()

    mock_stub = MagicMock()
    mock_stub.AssignJob.side_effect = _rpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)

    with patch("coordinator.job_router.db.get_all_workers",
               return_value=[{"worker_id": "worker-1", "address": "worker-1", "port": 50051}]), \
         patch("coordinator.job_router.db.update_job_assigned", side_effect=Exception("db is down")), \
         patch("coordinator.job_router.scheduler_pb2_grpc.WorkerServiceStub", return_value=mock_stub), \
         patch("coordinator.job_router.grpc.insecure_channel") as mock_channel:
        mock_channel.return_value.__enter__.return_value = MagicMock()

        try:
            router.route("job-1", "echo hi")
            raise AssertionError("expected RuntimeError to be raised")
        except RuntimeError as e:
            # Still the original gRPC-unreachable error, not the DB error.
            assert "unreachable" in str(e).lower()

    print("PASSED")


def test_other_rpc_errors_do_not_remove_worker():
    print("\n--- TEST 3: A non-connectivity gRPC error leaves the worker in the ring ---")
    router, ring = _router_with_one_worker()

    mock_stub = MagicMock()
    mock_stub.AssignJob.side_effect = _rpc_error(grpc.StatusCode.INTERNAL, "worker blew up")

    with patch("coordinator.job_router.db.get_all_workers",
               return_value=[{"worker_id": "worker-1", "address": "worker-1", "port": 50051}]), \
         patch("coordinator.job_router.scheduler_pb2_grpc.WorkerServiceStub", return_value=mock_stub), \
         patch("coordinator.job_router.grpc.insecure_channel") as mock_channel:
        mock_channel.return_value.__enter__.return_value = MagicMock()

        try:
            router.route("job-1", "echo hi")
            assert False, "expected RuntimeError to be raised"
        except RuntimeError as e:
            assert "worker blew up" in str(e)

    # INTERNAL isn't a connectivity problem — don't punish the worker for it.
    assert "worker-1" in ring.get_all_workers()
    print("PASSED")


def test_successful_route_returns_worker_id():
    print("\n--- TEST 4: Successful AssignJob returns the worker id, keeps it in the ring ---")
    router, ring = _router_with_one_worker()

    mock_response = MagicMock(status="accepted")
    mock_stub = MagicMock()
    mock_stub.AssignJob.return_value = mock_response

    with patch("coordinator.job_router.db.get_all_workers",
               return_value=[{"worker_id": "worker-1", "address": "worker-1", "port": 50051}]), \
         patch("coordinator.job_router.db.update_job_assigned") as mock_update, \
         patch("coordinator.job_router.scheduler_pb2_grpc.WorkerServiceStub", return_value=mock_stub), \
         patch("coordinator.job_router.grpc.insecure_channel") as mock_channel:
        mock_channel.return_value.__enter__.return_value = MagicMock()

        result = router.route("job-1", "echo hi")

    assert result == "worker-1"
    mock_update.assert_called_once_with("job-1", "worker-1")
    assert "worker-1" in ring.get_all_workers()
    print("PASSED")


if __name__ == "__main__":
    test_deadline_exceeded_removes_worker_from_ring()
    test_unavailable_removes_worker_from_ring()
    test_recovery_record_failure_does_not_mask_original_error()
    test_other_rpc_errors_do_not_remove_worker()
    test_successful_route_returns_worker_id()
    print("\n✅ All tests passed — job_router timeout/unreachable handling is working correctly")
