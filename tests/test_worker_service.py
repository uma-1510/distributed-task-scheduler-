# Unit tests for worker/main.py's WorkerService — bounded job execution
# (issue #9). Mocks execute_job so these run without actually spawning
# subprocesses.

import sys
import threading
import time
from unittest.mock import MagicMock, patch

sys.path.insert(0, '.')

from worker.main import WorkerService, JOB_QUEUE_WARNING_THRESHOLD


def _request(job_id="job-1", command="echo hi"):
    return MagicMock(job_id=job_id, command=command)


def test_assign_job_submits_to_bounded_executor_not_raw_threads():
    print("\n--- TEST 1: AssignJob submits to the executor, doesn't spawn raw threads ---")
    with patch("worker.main.execute_job") as mock_execute:
        service = WorkerService()
        try:
            response = service.AssignJob(_request("job-1"), None)
            # Give the pooled thread a moment to actually run the submitted task.
            for _ in range(50):
                if mock_execute.called:
                    break
                time.sleep(0.02)
        finally:
            service.shutdown()

    assert response.job_id == "job-1"
    assert response.status == "accepted"
    # execute_job is now called with the register/unregister proc hooks too
    # (see the executor-termination fix) — assert on the job args, and that
    # both hooks passed are the service's own bound methods.
    mock_execute.assert_called_once()
    call_args = mock_execute.call_args.args
    assert call_args[0] == "job-1"
    assert call_args[1] == "echo hi"
    assert call_args[2] == service._register_proc
    assert call_args[3] == service._unregister_proc
    print("PASSED")


def test_executor_respects_max_workers_cap():
    print("\n--- TEST 2: concurrent executions never exceed WORKER_MAX_THREADS ---")
    max_concurrent = 0
    current = 0
    lock = threading.Lock()
    release = threading.Event()

    def slow_execute(job_id, command, register_proc=None, unregister_proc=None):
        nonlocal max_concurrent, current
        with lock:
            current += 1
            max_concurrent = max(max_concurrent, current)
        release.wait(timeout=2)
        with lock:
            current -= 1

    with patch("worker.main.execute_job", side_effect=slow_execute), \
         patch("worker.main.WORKER_MAX_THREADS", 3):
        service = WorkerService()
        try:
            # Submit far more jobs than the cap.
            for i in range(10):
                service.AssignJob(_request(f"job-{i}"), None)
            time.sleep(0.3)  # let the pool fill up to its cap
            release.set()    # let all submitted jobs finish
        finally:
            service.shutdown()

    assert max_concurrent <= 3, f"expected at most 3 concurrent executions, saw {max_concurrent}"
    print(f"PASSED (max concurrent observed: {max_concurrent})")


def test_shutdown_cancels_queued_but_not_started_jobs():
    print("\n--- TEST 3: shutdown() cancels queued jobs, only the running one actually executes ---")
    started = threading.Event()
    block = threading.Event()
    executed_job_ids = []
    executed_lock = threading.Lock()

    def blocking_execute(job_id, command, register_proc=None, unregister_proc=None):
        with executed_lock:
            executed_job_ids.append(job_id)
        started.set()
        block.wait(timeout=2)

    with patch("worker.main.execute_job", side_effect=blocking_execute), \
         patch("worker.main.WORKER_MAX_THREADS", 1):
        service = WorkerService()
        service.AssignJob(_request("job-running"), None)
        started.wait(timeout=2)  # first job now occupies the only worker thread
        service.AssignJob(_request("job-queued"), None)  # this one just sits in queue

        service.shutdown()  # should cancel the queued one without hanging
        block.set()  # let the running one finish so its thread can exit

        # Observe the actual outcome instead of just trusting shutdown()
        # didn't hang: wait for the running job's thread to fully exit, then
        # assert exactly what ran.
        service._job_executor.shutdown(wait=True)

    assert executed_job_ids == ["job-running"], (
        f"expected only job-running to execute, got {executed_job_ids} — "
        "job-queued should have been cancelled by shutdown() before it started"
    )
    print("PASSED")


def test_queue_depth_warning_fires_above_threshold():
    print("\n--- TEST 4: the queue-depth warning actually fires when the backlog exceeds the threshold ---")
    block = threading.Event()

    def blocking_execute(job_id, command, register_proc=None, unregister_proc=None):
        block.wait(timeout=2)

    with patch("worker.main.execute_job", side_effect=blocking_execute), \
         patch("worker.main.WORKER_MAX_THREADS", 1), \
         patch("worker.main.JOB_QUEUE_WARNING_THRESHOLD", 2), \
         patch("builtins.print") as mock_print:
        service = WorkerService()
        try:
            # First job occupies the only worker thread; the rest queue up.
            # The depth check happens BEFORE each new job is added, so with
            # threshold=2 it takes 6 submissions to actually observe a
            # checked depth > 2 (job-0 runs immediately; job-1..job-5 queue,
            # and the check for job-5 sees a queue depth of 3).
            for i in range(6):
                service.AssignJob(_request(f"job-{i}"), None)
                time.sleep(0.05)
        finally:
            block.set()
            service.shutdown()
            service._job_executor.shutdown(wait=True)

    warned = any(
        "queue depth" in str(call.args[0]) and "⚠️" in str(call.args[0])
        for call in mock_print.call_args_list
    )
    assert warned, "expected the backlog warning to be printed once queue depth exceeded the threshold"
    print("PASSED")


def test_shutdown_terminates_in_flight_subprocess():
    print("\n--- TEST 5: shutdown() terminates a job that already has a live subprocess ---")
    with patch("worker.main.execute_job"):
        service = WorkerService()

    fake_proc = MagicMock()
    service._register_proc("job-running", fake_proc)

    service.shutdown()

    fake_proc.terminate.assert_called_once()
    print("PASSED")


if __name__ == "__main__":
    test_assign_job_submits_to_bounded_executor_not_raw_threads()
    test_executor_respects_max_workers_cap()
    test_shutdown_cancels_queued_but_not_started_jobs()
    test_queue_depth_warning_fires_above_threshold()
    test_shutdown_terminates_in_flight_subprocess()
    print("\n✅ All tests passed — worker bounded thread pool is working correctly")
