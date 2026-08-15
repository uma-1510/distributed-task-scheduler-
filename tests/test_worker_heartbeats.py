# Unit tests for worker/main.py's decoupled heartbeat sending (issue #2).
# Mocks requests.post and the heartbeat executor so these run without a
# live coordinator.
#
# Backlog depth is tracked as a module-level counter (worker.main.
# _heartbeat_backlog_count) guarded by a lock, rather than reading the
# executor's private _work_queue — see review feedback on #38. Tests set
# that counter directly instead of mocking a private attribute.

import sys
from unittest.mock import patch

sys.path.insert(0, '.')

import worker.main as worker_main
from worker.main import (
    HEARTBEAT_QUEUE_MAX_DEPTH,
    HEARTBEAT_QUEUE_WARNING_THRESHOLD,
    HEARTBEAT_SEND_TIMEOUT_SECONDS,
    _send_one_heartbeat,
    send_heartbeats,
)


def _set_backlog(n):
    worker_main._heartbeat_backlog_count = n


def test_send_one_heartbeat_posts_with_the_longer_timeout():
    print("\n--- TEST 1: _send_one_heartbeat uses HEARTBEAT_SEND_TIMEOUT_SECONDS ---")
    _set_backlog(1)
    with patch("worker.main.requests.post") as mock_post:
        _send_one_heartbeat("worker-1")

    mock_post.assert_called_once()
    assert mock_post.call_args.kwargs["timeout"] == HEARTBEAT_SEND_TIMEOUT_SECONDS
    print("PASSED")


def test_send_one_heartbeat_swallows_request_exceptions():
    print("\n--- TEST 2: a failed request doesn't raise out of _send_one_heartbeat ---")
    _set_backlog(1)
    with patch("worker.main.requests.post", side_effect=ConnectionError("coordinator unreachable")):
        _send_one_heartbeat("worker-1")  # should not raise
    print("PASSED")


def test_send_one_heartbeat_decrements_backlog_even_on_failure():
    print("\n--- TEST 3: backlog count decrements after send, success or failure ---")
    _set_backlog(3)
    with patch("worker.main.requests.post"):
        _send_one_heartbeat("worker-1")
    assert worker_main._heartbeat_backlog_count == 2

    _set_backlog(3)
    with patch("worker.main.requests.post", side_effect=ConnectionError("down")):
        _send_one_heartbeat("worker-1")
    assert worker_main._heartbeat_backlog_count == 2
    print("PASSED")


def test_send_heartbeats_submits_to_executor_instead_of_calling_directly():
    print("\n--- TEST 4: send_heartbeats() submits work to the executor, not a blocking call ---")
    _set_backlog(0)
    with patch("worker.main.heartbeat_executor") as mock_executor, \
         patch("worker.main.time.sleep", side_effect=[None, StopIteration]):
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
    print("\n--- TEST 5: at max queue depth, no new ping is submitted this cycle ---")
    _set_backlog(HEARTBEAT_QUEUE_MAX_DEPTH)
    with patch("worker.main.heartbeat_executor") as mock_executor, \
         patch("worker.main.time.sleep", side_effect=[StopIteration]):
        try:
            send_heartbeats("worker-1")
        except StopIteration:
            pass

    mock_executor.submit.assert_not_called()
    # Depth shouldn't have been bumped past the cap either.
    assert worker_main._heartbeat_backlog_count == HEARTBEAT_QUEUE_MAX_DEPTH
    print("PASSED")


def test_send_heartbeats_submits_below_max_queue_depth():
    print("\n--- TEST 6: below max queue depth, the ping still gets submitted, count increments ---")
    _set_backlog(HEARTBEAT_QUEUE_MAX_DEPTH - 1)
    with patch("worker.main.heartbeat_executor") as mock_executor, \
         patch("worker.main.time.sleep", side_effect=[StopIteration]):
        try:
            send_heartbeats("worker-1")
        except StopIteration:
            pass

    mock_executor.submit.assert_called_once()
    assert worker_main._heartbeat_backlog_count == HEARTBEAT_QUEUE_MAX_DEPTH
    print("PASSED")


def test_send_heartbeats_logs_warning_above_threshold():
    print("\n--- TEST 7: warning log fires once backlog exceeds HEARTBEAT_QUEUE_WARNING_THRESHOLD ---")
    _set_backlog(HEARTBEAT_QUEUE_WARNING_THRESHOLD + 1)
    with patch("worker.main.heartbeat_executor"), \
         patch("worker.main.time.sleep", side_effect=[StopIteration]), \
         patch("builtins.print") as mock_print:
        try:
            send_heartbeats("worker-1")
        except StopIteration:
            pass

    warned = any("backed up" in str(call.args[0]) for call in mock_print.call_args_list)
    assert warned, "expected a backlog warning to be printed above the threshold"
    print("PASSED")


def test_send_heartbeats_does_not_warn_at_or_below_threshold():
    print("\n--- TEST 8: no warning log at or below the threshold ---")
    _set_backlog(HEARTBEAT_QUEUE_WARNING_THRESHOLD)
    with patch("worker.main.heartbeat_executor"), \
         patch("worker.main.time.sleep", side_effect=[StopIteration]), \
         patch("builtins.print") as mock_print:
        try:
            send_heartbeats("worker-1")
        except StopIteration:
            pass

    warned = any("backed up" in str(call.args[0]) for call in mock_print.call_args_list)
    assert not warned, "should not warn when backlog is at or below the threshold"
    print("PASSED")


if __name__ == "__main__":
    test_send_one_heartbeat_posts_with_the_longer_timeout()
    test_send_one_heartbeat_swallows_request_exceptions()
    test_send_one_heartbeat_decrements_backlog_even_on_failure()
    test_send_heartbeats_submits_to_executor_instead_of_calling_directly()
    test_send_heartbeats_skips_submitting_at_max_queue_depth()
    test_send_heartbeats_submits_below_max_queue_depth()
    test_send_heartbeats_logs_warning_above_threshold()
    test_send_heartbeats_does_not_warn_at_or_below_threshold()
    print("\n✅ All tests passed — worker heartbeat decoupling is working correctly")
