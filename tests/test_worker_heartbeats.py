# Unit tests for worker/main.py's decoupled heartbeat sending (issue #2).
# Mocks requests.post and the heartbeat executor so these run without a
# live coordinator.

import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, '.')

from worker.main import (
    HEARTBEAT_QUEUE_MAX_DEPTH,
    HEARTBEAT_SEND_TIMEOUT_SECONDS,
    _send_one_heartbeat,
    send_heartbeats,
)


def test_send_one_heartbeat_posts_with_the_longer_timeout():
    print("\n--- TEST 1: _send_one_heartbeat uses HEARTBEAT_SEND_TIMEOUT_SECONDS ---")
    with patch("worker.main.requests.post") as mock_post:
        _send_one_heartbeat("worker-1")

    mock_post.assert_called_once()
    assert mock_post.call_args.kwargs["timeout"] == HEARTBEAT_SEND_TIMEOUT_SECONDS
    print("PASSED")


def test_send_one_heartbeat_swallows_request_exceptions():
    print("\n--- TEST 2: a failed request doesn't raise out of _send_one_heartbeat ---")
    with patch("worker.main.requests.post", side_effect=ConnectionError("coordinator unreachable")):
        _send_one_heartbeat("worker-1")  # should not raise
    print("PASSED")


def test_send_heartbeats_submits_to_executor_instead_of_calling_directly():
    print("\n--- TEST 3: send_heartbeats() submits work to the executor, not a blocking call ---")
    with patch("worker.main.heartbeat_executor") as mock_executor, \
         patch("worker.main.time.sleep", side_effect=[None, StopIteration]):
        mock_executor._work_queue.qsize.return_value = 0
        try:
            send_heartbeats("worker-1")
        except StopIteration:
            pass

    assert mock_executor.submit.call_count >= 1
    fn, worker_id = mock_executor.submit.call_args_list[0][0]
    assert fn is _send_one_heartbeat
    assert worker_id == "worker-1"
    print("PASSED")


def test_send_heartbeats_skips_submitting_at_max_queue_depth():
    print("\n--- TEST 4: at max queue depth, no new ping is submitted this cycle ---")
    with patch("worker.main.heartbeat_executor") as mock_executor, \
         patch("worker.main.time.sleep", side_effect=[StopIteration]):
        mock_executor._work_queue.qsize.return_value = HEARTBEAT_QUEUE_MAX_DEPTH
        try:
            send_heartbeats("worker-1")
        except StopIteration:
            pass

    mock_executor.submit.assert_not_called()
    print("PASSED")


def test_send_heartbeats_submits_below_max_queue_depth():
    print("\n--- TEST 5: below max queue depth, the ping still gets submitted ---")
    with patch("worker.main.heartbeat_executor") as mock_executor, \
         patch("worker.main.time.sleep", side_effect=[StopIteration]):
        mock_executor._work_queue.qsize.return_value = HEARTBEAT_QUEUE_MAX_DEPTH - 1
        try:
            send_heartbeats("worker-1")
        except StopIteration:
            pass

    mock_executor.submit.assert_called_once()
    print("PASSED")


if __name__ == "__main__":
    test_send_one_heartbeat_posts_with_the_longer_timeout()
    test_send_one_heartbeat_swallows_request_exceptions()
    test_send_heartbeats_submits_to_executor_instead_of_calling_directly()
    test_send_heartbeats_skips_submitting_at_max_queue_depth()
    test_send_heartbeats_submits_below_max_queue_depth()
    print("\n✅ All tests passed — worker heartbeat decoupling is working correctly")
