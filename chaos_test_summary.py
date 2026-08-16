"""
chaos_test_summary.py

Turns a chaos_test.py results JSON file (see chaos_test.py's --output) into a
readable markdown summary: pass/fail/skip counts, success rate, average
reassignment latency, and a breakdown by which worker was killed.

Usage:
    python3 chaos_test_summary.py chaos_test_results.json
    python3 chaos_test_summary.py chaos_test_results.json --output chaos_test_summary.md
"""

import argparse
import json
from pathlib import Path

# "skipped" runs never actually killed a worker (the job didn't land on the
# target), so they're excluded from the success rate — they're not failures
# of the thing being tested, just a routing miss.
ATTEMPTED_STATUSES = {"passed", "failed", "error"}


def load_results(path):
    with open(path) as f:
        return json.load(f)


def compute_summary(data):
    results = data.get("results", [])

    passed = [r for r in results if r["status"] == "passed"]
    failed = [r for r in results if r["status"] == "failed"]
    errored = [r for r in results if r["status"] == "error"]
    skipped = [r for r in results if r["status"] == "skipped"]
    attempted = [r for r in results if r["status"] in ATTEMPTED_STATUSES]

    success_rate_pct = (len(passed) / len(attempted) * 100) if attempted else 0.0

    latencies = [
        r["reassignment_latency_seconds"]
        for r in passed
        if r.get("reassignment_latency_seconds") is not None
    ]
    avg_latency = sum(latencies) / len(latencies) if latencies else None

    per_worker = {}
    for r in attempted:
        # Group by target_worker (always present — who this run was aiming
        # at) rather than worker_killed, which is only added once the kill
        # actually succeeds. An "error" record from a failure before the
        # kill step (e.g. submit_job couldn't reach the coordinator) has no
        # worker_killed key at all, so keying on it here would KeyError.
        worker_key = r.get("target_worker", r.get("worker_killed"))
        bucket = per_worker.setdefault(
            worker_key, {"passed": 0, "failed": 0, "errored": 0}
        )
        key = "errored" if r["status"] == "error" else r["status"]
        bucket[key] += 1

    return {
        "total_runs": len(results),
        "attempted": len(attempted),
        "passed": len(passed),
        "failed": len(failed),
        "errored": len(errored),
        "skipped": len(skipped),
        "success_rate_pct": round(success_rate_pct, 1),
        "avg_reassignment_latency_seconds": (
            round(avg_latency, 2) if avg_latency is not None else None
        ),
        "per_worker": per_worker,
    }


def to_markdown(summary):
    latency = summary["avg_reassignment_latency_seconds"]
    lines = [
        "## Chaos Test Summary",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Total runs | {summary['total_runs']} |",
        f"| Attempted (excludes skipped) | {summary['attempted']} |",
        f"| Passed | {summary['passed']} |",
        f"| Failed | {summary['failed']} |",
        f"| Errored | {summary['errored']} |",
        f"| Skipped | {summary['skipped']} |",
        f"| Success rate | {summary['success_rate_pct']}% |",
        f"| Avg reassignment latency | {f'{latency}s' if latency is not None else 'N/A'} |",
    ]

    if summary["per_worker"]:
        lines += [
            "",
            "### By worker killed",
            "",
            "| Worker | Passed | Failed | Errored |",
            "|---|---|---|---|",
        ]
        for worker, counts in sorted(summary["per_worker"].items()):
            lines.append(
                f"| {worker} | {counts['passed']} | {counts['failed']} | {counts['errored']} |"
            )

    return "\n".join(lines) + "\n"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Summarize a chaos_test.py results JSON file as a markdown table."
    )
    parser.add_argument("results_file", type=str, help="path to a chaos_test.py results JSON file")
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="write markdown to this file instead of printing to stdout",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    data = load_results(args.results_file)
    summary = compute_summary(data)
    markdown = to_markdown(summary)

    if args.output:
        Path(args.output).write_text(markdown)
        print(f"summary written to {args.output}")
    else:
        print(markdown)


if __name__ == "__main__":
    main()
