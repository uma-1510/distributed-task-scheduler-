# Runs jobs, captures stdout/stderr

import subprocess
import sys
sys.path.insert(0, '.')

from coordinator import database as db


def execute_job(job_id: str, command: str, register_proc=None, unregister_proc=None):
    """register_proc(job_id, proc) / unregister_proc(job_id) are optional
    hooks so a caller (WorkerService) can track which subprocess belongs to
    which in-flight job — see issue #9's follow-up review. Without this,
    there was no way to actually stop a job that was already running when
    the worker shuts down; executor.shutdown() could only cancel
    queued-but-not-started futures, not kill a live subprocess."""
    print(f"[executor] starting job {job_id}: {command}")
    db.update_job_running(job_id)

    try:
        proc = subprocess.Popen(
            command, shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            # shell=True runs the command as a CHILD of /bin/sh — terminating
            # just the shell (proc.terminate()) doesn't kill whatever the
            # shell itself spawned (e.g. a `sleep 60` grandchild), which
            # keeps running and keeps the stdout pipe open. Found this via
            # live testing: shutdown() correctly called terminate(), but the
            # worker still didn't exit and got SIGKILLed by Docker instead.
            # start_new_session puts the shell in its own process group so
            # the whole group (shell + everything it launched) can be
            # signaled together with os.killpg() — see worker/main.py.
            start_new_session=True,
        )
        if register_proc:
            register_proc(job_id, proc)
        try:
            stdout, _ = proc.communicate()
            exit_code = proc.returncode
        finally:
            if unregister_proc:
                unregister_proc(job_id)
    except Exception as e:
        stdout    = str(e)
        exit_code = -1

    db.update_job_completed(job_id, stdout, exit_code)
    status = "completed" if exit_code == 0 else "failed"
    print(f"[executor] job {job_id} {status} (exit {exit_code})")