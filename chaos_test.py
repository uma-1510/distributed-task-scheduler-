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
import subprocess
import time

import requests

COORDINATOR_URL = "http://localhost:8000"
REASSIGNMENT_WAIT_SECONDS = 25


def submit_job(command):
    r = requests.post(f"{COORDINATOR_URL}/jobs", json={"command": command})
    r.raise_for_status()
    return r.json()


def get_job(job_id):
    r = requests.get(f"{COORDINATOR_URL}/jobs/{job_id}")
    r.raise_for_status()
    return r.json()


def kill_worker(worker_name):
    subprocess.run(["docker", "compose", "stop", worker_name])


def restart_worker(worker_name):
    subprocess.run(["docker", "compose", "start", worker_name])


def run_once(run_number, total_runs, worker_to_kill, sleep_seconds):
    print(f"\n=== Run {run_number}/{total_runs} (target: {worker_to_kill}) ===")

    job = submit_job(f"sleep {sleep_seconds} && echo chaos_run_{run_number}")
    job_id = job["job_id"]
    print(f"submitted {job_id} -> {job['worker_id']}")

    if job["worker_id"] != worker_to_kill:
        print(f"job landed on {job['worker_id']}, not {worker_to_kill} — skipping this run")
        return None

    time.sleep(2)
    print(f"killing {worker_to_kill}...")
    kill_worker(worker_to_kill)

    reassigned = False
    for elapsed in range(REASSIGNMENT_WAIT_SECONDS):
        time.sleep(1)
        job_state = get_job(job_id)
        assigned_to = job_state["assigned_to"]
        if assigned_to and assigned_to != worker_to_kill:
            print(f"reassigned to {assigned_to} after {elapsed + 1}s")
            reassigned = True
            break

    if not reassigned:
        print(f"job was NOT reassigned within {REASSIGNMENT_WAIT_SECONDS}s")

    print(f"restarting {worker_to_kill}...")
    restart_worker(worker_to_kill)
    time.sleep(5)  # give it time to re-register before the next run

    return reassigned


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

    completed = [r for r in results if r is not None]
    passed = completed.count(True)
    print(f"\n{passed}/{len(completed)} runs passed ({results.count(None)} skipped)")


if __name__ == "__main__":
    main()
