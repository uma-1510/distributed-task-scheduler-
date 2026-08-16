# Unit tests for chaos_test_summary.py's parsing/summary logic.
#
# Deliberately NOT testing chaos_test.py's live chaos run here — that needs a
# running docker-compose cluster (coordinator + workers) and is exercised
# manually / in CI as an integration step instead, the same way
# tests/test_failure.py is. This file covers the pure functions that turn a
# results JSON file into the markdown summary, which don't need a cluster.

import json
import math
import os
import tempfile

from chaos_test_summary import compute_summary, load_results, to_markdown

# Run with `python -m pytest tests/test_chaos_summary.py` (or `python -m
# tests.test_chaos_summary`) from the repo root — pytest's import mode
# resolves the top-level chaos_test_summary module via tests/__init__.py
# without needing to mutate sys.path here.


def test_compute_summary_basic():
    print("\n--- TEST 1: Basic pass/fail/skip/error counts ---")
    data = {
        "results": [
            {"run": 1, "worker_killed": "worker-1", "status": "passed",
             "reassigned_to": "worker-2", "reassignment_latency_seconds": 10.0},
            {"run": 2, "worker_killed": "worker-1", "status": "passed",
             "reassigned_to": "worker-3", "reassignment_latency_seconds": 20.0},
            {"run": 3, "worker_killed": "worker-2", "status": "failed",
             "reason": "not reassigned within 25s"},
            {"run": 4, "worker_killed": "worker-2", "status": "skipped",
             "reason": "routing miss"},
            {"run": 5, "worker_killed": "worker-3", "status": "error",
             "reason": "docker CLI not found"},
        ]
    }
    summary = compute_summary(data)
    assert summary["total_runs"] == 5
    assert summary["attempted"] == 4, "skipped run shouldn't count as attempted"
    assert summary["passed"] == 2
    assert summary["failed"] == 1
    assert summary["errored"] == 1
    assert summary["skipped"] == 1
    assert math.isclose(summary["success_rate_pct"], 50.0)
    assert math.isclose(summary["avg_reassignment_latency_seconds"], 15.0)
    print("PASSED")


def test_compute_summary_empty_results():
    print("\n--- TEST 2: Empty results don't divide by zero ---")
    summary = compute_summary({"results": []})
    assert summary["total_runs"] == 0
    assert summary["attempted"] == 0
    assert math.isclose(summary["success_rate_pct"], 0.0)
    assert summary["avg_reassignment_latency_seconds"] is None
    print("PASSED")


def test_compute_summary_all_skipped():
    print("\n--- TEST 3: All-skipped results don't divide by zero ---")
    data = {"results": [{"run": 1, "worker_killed": "worker-1", "status": "skipped",
                          "reason": "routing miss"}]}
    summary = compute_summary(data)
    assert summary["attempted"] == 0
    assert math.isclose(summary["success_rate_pct"], 0.0)
    print("PASSED")


def test_compute_summary_per_worker_breakdown():
    print("\n--- TEST 4: Per-worker breakdown ---")
    data = {
        "results": [
            {"run": 1, "worker_killed": "worker-1", "status": "passed",
             "reassignment_latency_seconds": 5.0},
            {"run": 2, "worker_killed": "worker-1", "status": "failed"},
            {"run": 3, "worker_killed": "worker-2", "status": "error"},
        ]
    }
    summary = compute_summary(data)
    assert summary["per_worker"]["worker-1"] == {"passed": 1, "failed": 1, "errored": 0}
    assert summary["per_worker"]["worker-2"] == {"passed": 0, "failed": 0, "errored": 1}
    print("PASSED")


def test_to_markdown_contains_key_fields():
    print("\n--- TEST 5: Markdown includes headline metrics ---")
    summary = compute_summary({
        "results": [
            {"run": 1, "worker_killed": "worker-1", "status": "passed",
             "reassignment_latency_seconds": 12.5},
        ]
    })
    md = to_markdown(summary)
    assert "Chaos Test Summary" in md
    assert "Success rate" in md
    assert math.isclose(summary["success_rate_pct"], 100.0)
    # Check the actual computed value made it into the markdown, rather than
    # hardcoding an expected format (e.g. "100.0%" vs "100%") that could
    # legitimately change independently of this test's intent.
    assert str(summary["success_rate_pct"]) in md
    assert "worker-1" in md
    print("PASSED")


def test_to_markdown_handles_no_latency_data():
    print("\n--- TEST 6: Markdown handles no passed runs gracefully ---")
    summary = compute_summary({"results": [{"run": 1, "worker_killed": "worker-1", "status": "failed"}]})
    md = to_markdown(summary)
    assert "N/A" in md, "no passed runs means no latency data to average"
    print("PASSED")


def test_load_results_roundtrip():
    print("\n--- TEST 7: load_results reads back what was written ---")
    fixture = {
        "results": [
            {"run": 1, "worker_killed": "worker-1", "status": "passed",
             "reassignment_latency_seconds": 9.0}
        ]
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(fixture, f)
        path = f.name
    try:
        assert load_results(path) == fixture
    finally:
        os.remove(path)
    print("PASSED")


def test_compute_summary_error_record_missing_worker_killed_key():
    print("\n--- TEST 8: an 'error' record with no worker_killed key doesn't crash the per-worker breakdown ---")
    # chaos_test.py only adds 'worker_killed' once kill_worker() actually
    # succeeds — an error before that point (e.g. submit_job can't reach
    # the coordinator) has 'target_worker' but no 'worker_killed' at all.
    data = {
        "results": [
            {"run": 1, "target_worker": "worker-1", "status": "error",
             "reason": "failed to submit job"},
        ]
    }
    summary = compute_summary(data)  # would raise KeyError before this fix
    assert summary["per_worker"]["worker-1"] == {"passed": 0, "failed": 0, "errored": 1}
    print("PASSED")


if __name__ == "__main__":
    test_compute_summary_basic()
    test_compute_summary_empty_results()
    test_compute_summary_all_skipped()
    test_compute_summary_per_worker_breakdown()
    test_to_markdown_contains_key_fields()
    test_to_markdown_handles_no_latency_data()
    test_load_results_roundtrip()
    test_compute_summary_error_record_missing_worker_killed_key()
    print("\n✅ All tests passed — chaos_test_summary logic is working correctly")
