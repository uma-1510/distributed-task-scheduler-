"""
chaos_test.py

Kills a worker mid-job, repeatedly, and checks that the coordinator detects
the failure (via the heartbeat monitor) and reassigns the job to a healthy
worker, instead of leaving it stuck.

This automates the manual steps in the README's "Simulate worker failure"
section using `docker compose stop/start` instead of a human killing a
process and watching curl output.
"""

import argparse
import json
import subprocess
import time
from pathlib import Path

import requests

COORDINATOR_URL = "http://localhost:8000"
REASSIGNMENT_WAIT_SECONDS = 25
HTTP_TIMEOUT_SECONDS = 10
DOCKER_TIMEOUT_SECONDS = 30


class ChaosTestError(Exception):
    """A chaos-test step failed in a way that should abort the current run
    (coordinator unreachable, Docker not running, etc.) rather than crash
    the whole script."""


def submit_job(command):
    try:
        r = requests.post(
            f"{COORDINATOR_URL}/jobs", json={"command": command}, timeout=HTTP_TIMEOUT_SECONDS
        )
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        raise ChaosTestError(f"failed to submit job (is the coordinator up? {COORDINATOR_URL}): {e}") from e


def get_job(job_id):
    try:
        r = requests.get(f"{COORDINATOR_URL}/jobs/{job_id}", timeout=HTTP_TIMEOUT_SECONDS)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        raise ChaosTestError(f"failed to fetch job {job_id}: {e}") from e


def _run_docker_compose(*args, worker_name):
    try:
        result = subprocess.run(
            ["docker", "compose", *args, worker_name],
            capture_output=True,
            text=True,
            timeout=DOCKER_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as e:
        raise ChaosTestError("docker CLI not found — is Docker installed and on PATH?") from e
    except OSError as e:
        # Covers PermissionError and other OS-level failures starting the
        # docker CLI that aren't FileNotFoundError — without this they'd
        # still crash the whole script with a raw traceback.
        raise ChaosTestError(f"failed to start docker CLI: {e}") from e
    except subprocess.TimeoutExpired as e:
        raise ChaosTestError(f"'docker compose {' '.join(args)} {worker_name}' timed out") from e

    if result.returncode != 0:
        raise ChaosTestError(
            f"'docker compose {' '.join(args)} {worker_name}' failed: {result.stderr.strip()}"
        )


def kill_worker(worker_name):
    _run_docker_compose("stop", worker_name=worker_name)


def restart_worker(worker_name):
    _run_docker_compose("start", worker_name=worker_name)


def run_once(run_number, total_runs, worker_to_kill, sleep_seconds):
    print(f"\n=== Run {run_number}/{total_runs} (target: {worker_to_kill}) ===")

    # "target_worker" is who we intended to kill; "worker_killed" only gets
    # added once kill_worker() actually succeeds below. Before this fix, a
    # skipped run's record still claimed worker_killed=target even though
    # nothing was ever killed — every skip in the committed results looked
    # like a kill.
    result = {"run": run_number, "target_worker": worker_to_kill}

    # Only true once kill_worker() is actually attempted — a failure before
    # that (e.g. submit_job can't reach the coordinator) has no worker down
    # to recover from, so the except block below shouldn't restart anything.
    recovery_needed = False

    try:
        job = submit_job(f"sleep {sleep_seconds} && echo chaos_run_{run_number}")
        job_id = job["job_id"]
        result["job_id"] = job_id
        print(f"submitted {job_id} -> {job['worker_id']}")

        if job["worker_id"] != worker_to_kill:
            print(f"job landed on {job['worker_id']}, not {worker_to_kill} — skipping this run")
            result["status"] = "skipped"
            result["reason"] = f"job routed to {job['worker_id']} instead of {worker_to_kill}"
            return result

        time.sleep(2)
        print(f"killing {worker_to_kill}...")
        recovery_needed = True  # set before the call — a Docker timeout here leaves the worker state unknown
        kill_worker(worker_to_kill)
        result["worker_killed"] = worker_to_kill
        kill_time = time.monotonic()

        reassigned = False
        assigned_to = None
        reassignment_latency_seconds = None
        for elapsed in range(REASSIGNMENT_WAIT_SECONDS):
            time.sleep(1)
            job_state = get_job(job_id)
            assigned_to = job_state["assigned_to"]
            if assigned_to and assigned_to != worker_to_kill:
                # Capture latency right here, at actual detection time — not
                # after restart_worker() + the 5s sleep below, which would
                # inflate every recorded latency by ~5-7s for no reason.
                reassignment_latency_seconds = round(time.monotonic() - kill_time, 2)
                print(f"reassigned to {assigned_to} after {elapsed + 1}s")
                reassigned = True
                break

        print(f"restarting {worker_to_kill}...")
        restart_worker(worker_to_kill)
        time.sleep(5)  # give it time to re-register before the next run

        if reassigned:
            result["status"] = "passed"
            result["reassigned_to"] = assigned_to
            result["reassignment_latency_seconds"] = reassignment_latency_seconds
        else:
            print(f"job was NOT reassigned within {REASSIGNMENT_WAIT_SECONDS}s")
            result["status"] = "failed"
            result["reason"] = f"not reassigned within {REASSIGNMENT_WAIT_SECONDS}s"

        return result

    except ChaosTestError as e:
        print(f"run {run_number} FAILED: {e}")
        # Best-effort: don't leave the worker down for the next run just
        # because this one hit an error partway through — but only if we
        # actually got as far as trying to kill it. A submit_job failure,
        # for example, never touched worker_to_kill at all.
        if recovery_needed:
            try:
                restart_worker(worker_to_kill)
            except ChaosTestError as restart_error:
                print(f"  also failed to restart {worker_to_kill}: {restart_error}")
        result["status"] = "error"
        result["reason"] = str(e)
        return result


def parse_args():
    parser = argparse.ArgumentParser(
        description="Chaos test: repeatedly kill workers and verify job reassignment."
    )
    parser.add_argument(
        "--runs", type=int, default=5, help="number of runs to execute (default: 5)"
    )
    parser.add_argument(
        "--workers",
        type=str,
        default="worker-1,worker-2,worker-3",
        help="comma-separated worker names to cycle through, one per run "
        "(default: worker-1,worker-2,worker-3)",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=20,
        help="how long each test job sleeps for, in seconds (default: 20)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="chaos_test_results.json",
        help="path to write the JSON results file to (default: chaos_test_results.json)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    workers = [w.strip() for w in args.workers.split(",") if w.strip()]
    if not workers:
        raise SystemExit("--workers must contain at least one worker name")

    print(f"Chaos test: {args.runs} runs, cycling through {workers}")

    results = [
        run_once(i, args.runs, workers[(i - 1) % len(workers)], args.sleep_seconds)
        for i in range(1, args.runs + 1)
    ]

    summary = {
        "passed": sum(1 for r in results if r["status"] == "passed"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "skipped": sum(1 for r in results if r["status"] == "skipped"),
        "errored": sum(1 for r in results if r["status"] == "error"),
    }
    print(
        f"\n{summary['passed']} passed, {summary['failed']} failed, "
        f"{summary['skipped']} skipped, {summary['errored']} errored "
        f"(of {len(results)} runs)"
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            {
                "runs": args.runs,
                "workers": workers,
                "sleep_seconds": args.sleep_seconds,
                "summary": summary,
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"results written to {output_path}")


if __name__ == "__main__":
    main()
