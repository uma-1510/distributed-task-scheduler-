# Runs jobs, captures stdout/stderr

import subprocess
import sys
sys.path.insert(0, '.')

from coordinator import database as db


def execute_job(job_id: str, command: str):
    print(f"[executor] starting job {job_id}: {command}")
    db.update_job_running(job_id)

    try:
        proc = subprocess.Popen(
            command, shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        stdout, _ = proc.communicate()
        exit_code  = proc.returncode
    except Exception as e:
        stdout    = str(e)
        exit_code = -1

    db.update_job_completed(job_id, stdout, exit_code)
    status = "completed" if exit_code == 0 else "failed"
    print(f"[executor] job {job_id} {status} (exit {exit_code})")