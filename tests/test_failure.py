import requests
import time
import subprocess
import sys

BASE = "http://localhost:8000"

def submit_job(command):
    r = requests.post(f"{BASE}/jobs", json={"command": command})
    r.raise_for_status()
    data = r.json()
    print(f"  submitted → {data['job_id']} assigned to {data['worker_id']}")
    return data["job_id"]

def get_job(job_id):
    r = requests.get(f"{BASE}/jobs/{job_id}")
    r.raise_for_status()
    return r.json()

def get_workers():
    r = requests.get(f"{BASE}/workers")
    r.raise_for_status()
    return r.json()

def print_workers():
    workers = get_workers()
    for w in workers:
        hb = w.get("last_heartbeat", "N/A")
        print(f"  {w['worker_id']} — status: {w['status']} — last_heartbeat: {hb}")

def print_jobs(job_ids):
    for job_id in job_ids:
        job = get_job(job_id)
        print(
            f"  {job_id[:8]}... "
            f"status={job['status']:<12} "
            f"assigned_to={job['assigned_to']}"
        )


print("\n=== STEP 1: Verify all 3 workers are healthy ===")
print_workers()

print("\n=== STEP 2: Submit 6 long-running jobs (sleep 30s each) ===")
job_ids = []
for i in range(6):
    jid = submit_job(f"sleep 30 && echo job_{i}_done")
    job_ids.append(jid)

time.sleep(2)
print("\nJob distribution before kill:")
print_jobs(job_ids)

print("\n=== STEP 3: Kill worker-2 NOW ===")
print("Run this in another terminal:")
print("")
print("  pkill -f 'WORKER_ID=worker-2'")
print("")
input("Press Enter here once you've killed worker-2...")

print("\n=== STEP 4: Waiting for monitor to detect failure (up to 20s) ===")
for i in range(20):
    time.sleep(1)
    workers = get_workers()
    dead = [w for w in workers if w["worker_id"] == "worker-2" and w["status"] == "dead"]
    if dead:
        print(f"  worker-2 marked DEAD after {i+1}s ✅")
        break
    print(f"  {i+1}s — worker-2 still showing: {[w['status'] for w in workers if w['worker_id'] == 'worker-2']}")

print("\n=== STEP 5: Checking job reassignment ===")
time.sleep(3)
print_jobs(job_ids)

print("\n=== STEP 6: Final worker states ===")
print_workers()

print("\n=== DONE ===")
print("Check that worker-2's jobs were reassigned to worker-1 or worker-3")