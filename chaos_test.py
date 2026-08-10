"""
chaos_test.py — v1

Kills a worker mid-job, repeatedly, and checks that the coordinator detects
the failure (via the heartbeat monitor) and reassigns the job to a healthy
worker, instead of leaving it stuck.

This automates the manual steps in the README's "Simulate worker failure"
section using `docker compose stop/start` instead of a human killing a
process and watching curl output.

v1: hardcoded worker/run count, no CLI args yet, minimal error handling.
"""

import subprocess
import time

import requests

COORDINATOR_URL = "http://localhost:8000"
WORKER_TO_KILL = "worker-2"
NUM_RUNS = 5
JOB_SLEEP_SECONDS = 20
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


def run_once(run_number):
    print(f"\n=== Run {run_number}/{NUM_RUNS} ===")

    job = submit_job(f"sleep {JOB_SLEEP_SECONDS} && echo chaos_run_{run_number}")
    job_id = job["job_id"]
    print(f"submitted {job_id} -> {job['worker_id']}")

    if job["worker_id"] != WORKER_TO_KILL:
        print(f"job landed on {job['worker_id']}, not {WORKER_TO_KILL} — skipping this run")
        return None

    time.sleep(2)
    print(f"killing {WORKER_TO_KILL}...")
    kill_worker(WORKER_TO_KILL)

    reassigned = False
    for elapsed in range(REASSIGNMENT_WAIT_SECONDS):
        time.sleep(1)
        job_state = get_job(job_id)
        assigned_to = job_state["assigned_to"]
        if assigned_to and assigned_to != WORKER_TO_KILL:
            print(f"reassigned to {assigned_to} after {elapsed + 1}s")
            reassigned = True
            break

    if not reassigned:
        print(f"job was NOT reassigned within {REASSIGNMENT_WAIT_SECONDS}s")

    print(f"restarting {WORKER_TO_KILL}...")
    restart_worker(WORKER_TO_KILL)
    time.sleep(5)  # give it time to re-register before the next run

    return reassigned


def main():
    print(f"Chaos test: killing {WORKER_TO_KILL} across {NUM_RUNS} runs")

    results = [run_once(i) for i in range(1, NUM_RUNS + 1)]

    completed = [r for r in results if r is not None]
    passed = completed.count(True)
    print(f"\n{passed}/{len(completed)} runs passed ({results.count(None)} skipped)")


if __name__ == "__main__":
    main()
