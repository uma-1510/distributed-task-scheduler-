# Unit tests for coordinator/main.py's job-submission backpressure: the
# sliding-window rate limiter, routing-queue-depth 429, and routing-timeout
# 504 (issue #8). Calls submit_job() directly (bypassing the ASGI/HTTP
# layer and FastAPI's lifespan, which needs a real Postgres connection) with
# db/router/executor mocked out.

import sys
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from unittest.mock import MagicMock, patch

sys.path.insert(0, '.')

from fastapi import HTTPException

from coordinator.main import (
    JOB_SUBMISSION_RATE_LIMIT,
    ROUTING_QUEUE_LIMIT,
    JobSubmit,
    SlidingWindowRateLimiter,
    submit_job,
)


def test_rate_limiter_allows_up_to_the_cap_then_rejects():
    print("\n--- TEST 1: rate limiter allows exactly max_per_window, rejects after ---")
    limiter = SlidingWindowRateLimiter(max_per_window=3, window_seconds=1.0)
    assert limiter.allow() is True
    assert limiter.allow() is True
    assert limiter.allow() is True
    assert limiter.allow() is False
    print("PASSED")


def test_rate_limiter_resets_after_window_expires():
    print("\n--- TEST 2: rate limiter allows again once the window passes ---")
    limiter = SlidingWindowRateLimiter(max_per_window=1, window_seconds=0.05)
    assert limiter.allow() is True
    assert limiter.allow() is False
    time.sleep(0.1)
    assert limiter.allow() is True
    print("PASSED")


def test_submit_job_returns_429_when_rate_limited():
    print("\n--- TEST 3: submit_job -> 429 when the rate limiter rejects ---")
    with patch("coordinator.main.submission_limiter.allow", return_value=False):
        try:
            submit_job(JobSubmit(command="echo hi"))
            assert False, "expected HTTPException"
        except HTTPException as e:
            assert e.status_code == 429
    print("PASSED")


def test_submit_job_returns_429_when_routing_queue_too_deep():
    print("\n--- TEST 4: submit_job -> 429 when the routing queue backlog is too deep ---")
    with patch("coordinator.main.submission_limiter.allow", return_value=True), \
         patch("coordinator.main.routing_executor") as mock_executor:
        mock_executor._work_queue.qsize.return_value = ROUTING_QUEUE_LIMIT + 1
        try:
            submit_job(JobSubmit(command="echo hi"))
            assert False, "expected HTTPException"
        except HTTPException as e:
            assert e.status_code == 429
    print("PASSED")


def test_submit_job_returns_504_on_routing_timeout():
    print("\n--- TEST 5: submit_job -> 504 when routing doesn't finish in time ---")
    with patch("coordinator.main.submission_limiter.allow", return_value=True), \
         patch("coordinator.main.db.create_job", return_value="job-1"), \
         patch("coordinator.main.routing_executor") as mock_executor:
        mock_executor._work_queue.qsize.return_value = 0
        mock_future = MagicMock()
        mock_future.result.side_effect = FutureTimeoutError()
        mock_executor.submit.return_value = mock_future

        try:
            submit_job(JobSubmit(command="sleep 100"))
            assert False, "expected HTTPException"
        except HTTPException as e:
            assert e.status_code == 504
    print("PASSED")


def test_submit_job_succeeds_and_returns_worker_id():
    print("\n--- TEST 6: happy path still returns the synchronous {job_id, status, worker_id} shape ---")
    with patch("coordinator.main.submission_limiter.allow", return_value=True), \
         patch("coordinator.main.db.create_job", return_value="job-1"), \
         patch("coordinator.main.routing_executor") as mock_executor:
        mock_executor._work_queue.qsize.return_value = 0
        mock_future = MagicMock()
        mock_future.result.return_value = "worker-2"
        mock_executor.submit.return_value = mock_future

        result = submit_job(JobSubmit(command="echo hi"))

    assert result == {"job_id": "job-1", "status": "assigned", "worker_id": "worker-2"}
    print("PASSED")


if __name__ == "__main__":
    test_rate_limiter_allows_up_to_the_cap_then_rejects()
    test_rate_limiter_resets_after_window_expires()
    test_submit_job_returns_429_when_rate_limited()
    test_submit_job_returns_429_when_routing_queue_too_deep()
    test_submit_job_returns_504_on_routing_timeout()
    test_submit_job_succeeds_and_returns_worker_id()
    print("\n✅ All tests passed — job submission backpressure is working correctly")
