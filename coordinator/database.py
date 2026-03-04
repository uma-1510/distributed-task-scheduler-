# PostgreSQL models + queries

import psycopg2
import psycopg2.extras
from datetime import datetime
from common.config import DATABASE_URL
from common.models import JobStatus, WorkerStatus


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def create_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS workers (
        worker_id       VARCHAR PRIMARY KEY,
        address         VARCHAR NOT NULL,
        port            INTEGER NOT NULL,
        status          VARCHAR NOT NULL DEFAULT 'healthy',
        last_heartbeat  TIMESTAMP,
        registered_at   TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS jobs (
        job_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        command       TEXT NOT NULL,
        status        VARCHAR NOT NULL DEFAULT 'pending',
        assigned_to   VARCHAR REFERENCES workers(worker_id),
        output        TEXT,
        exit_code     INTEGER,
        created_at    TIMESTAMP DEFAULT NOW(),
        started_at    TIMESTAMP,
        completed_at  TIMESTAMP
    );
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    print("[db] tables ready")


# ── Worker operations ──────────────────────────────────────────

def upsert_worker(worker_id: str, address: str, port: int):
    sql = """
    INSERT INTO workers (worker_id, address, port, status, last_heartbeat)
    VALUES (%s, %s, %s, 'healthy', NOW())
    ON CONFLICT (worker_id) DO UPDATE
        SET address = EXCLUDED.address,
            port    = EXCLUDED.port,
            status  = 'healthy',
            last_heartbeat = NOW();
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (worker_id, address, port))
        conn.commit()


def update_worker_heartbeat(worker_id: str):
    sql = "UPDATE workers SET last_heartbeat = NOW(), status = 'healthy' WHERE worker_id = %s;"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (worker_id,))
        conn.commit()


def update_worker_status(worker_id: str, status: WorkerStatus):
    sql = "UPDATE workers SET status = %s WHERE worker_id = %s;"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status.value, worker_id))
        conn.commit()


def get_all_workers() -> list[dict]:
    sql = "SELECT worker_id, address, port, status, last_heartbeat FROM workers;"
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]


def get_healthy_workers() -> list[dict]:
    sql = "SELECT * FROM workers WHERE status = 'healthy';"
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]


# ── Job operations ─────────────────────────────────────────────

def create_job(command: str) -> str:
    sql = """
    INSERT INTO jobs (command, status)
    VALUES (%s, 'pending')
    RETURNING job_id;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (command,))
            job_id = str(cur.fetchone()[0])
        conn.commit()
    return job_id


def update_job_assigned(job_id: str, worker_id: str):
    sql = """
    UPDATE jobs
    SET status = 'assigned', assigned_to = %s
    WHERE job_id = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (worker_id, job_id))
        conn.commit()


def update_job_running(job_id: str):
    sql = "UPDATE jobs SET status = 'running', started_at = NOW() WHERE job_id = %s;"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_id,))
        conn.commit()


def update_job_completed(job_id: str, output: str, exit_code: int):
    status = "completed" if exit_code == 0 else "failed"
    sql = """
    UPDATE jobs
    SET status = %s, output = %s, exit_code = %s, completed_at = NOW()
    WHERE job_id = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status, output, exit_code, job_id))
        conn.commit()


def get_job(job_id: str) -> dict | None:
    sql = "SELECT * FROM jobs WHERE job_id = %s;"
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (job_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def get_all_jobs() -> list[dict]:
    sql = "SELECT * FROM jobs ORDER BY created_at DESC;"
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]


def get_jobs_for_worker(worker_id: str, statuses: list[str]) -> list[dict]:
    sql = "SELECT * FROM jobs WHERE assigned_to = %s AND status = ANY(%s);"
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (worker_id, statuses))
            return [dict(row) for row in cur.fetchall()]