import psycopg2
import psycopg2.extras
from common.config import DATABASE_URL
from common.models import JobStatus, WorkerStatus


def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def create_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS workers (
        worker_id       VARCHAR PRIMARY KEY,
        address         VARCHAR NOT NULL,
        port            INTEGER NOT NULL,
        status          VARCHAR NOT NULL DEFAULT 'healthy',
        last_heartbeat  TIMESTAMPTZ,
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
    """)
    cur.close()
    conn.close()
    print("[db] tables ready")


def reset_all_workers():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE workers SET status = 'dead', last_heartbeat = NOW(), registered_at = NOW();")
    cur.close()
    conn.close()
    print("[db] all workers reset — timestamps refreshed")

def reset_worker_heartbeats():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE workers
        SET last_heartbeat = NOW(),
            status = 'alive'
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("[db] reset all worker heartbeats on startup")

def upsert_worker(worker_id: str, address: str, port: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO workers (worker_id, address, port, status, last_heartbeat, registered_at)
    VALUES (%s, %s, %s, 'healthy', NOW(), NOW())
    ON CONFLICT (worker_id) DO UPDATE
        SET address        = EXCLUDED.address,
            port           = EXCLUDED.port,
            status         = 'healthy',
            last_heartbeat = NOW(),
            registered_at  = NOW();
    """, (worker_id, address, port))
    cur.close()
    conn.close()


def update_worker_heartbeat(worker_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE workers SET last_heartbeat = NOW(), status = 'healthy' WHERE worker_id = %s;",
        (worker_id,)
    )
    cur.close()
    conn.close()


def update_worker_status(worker_id: str, status: WorkerStatus):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE workers SET status = %s WHERE worker_id = %s;",
        (status.value, worker_id)
    )
    cur.close()
    conn.close()


def get_all_workers() -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT worker_id, address, port, status, last_heartbeat, registered_at FROM workers;")
    rows = [dict(row) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def get_healthy_workers() -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT worker_id, address, port, status, last_heartbeat, registered_at FROM workers WHERE status = 'healthy';")
    rows = [dict(row) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def create_job(command: str) -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO jobs (command, status) VALUES (%s, 'pending') RETURNING job_id;",
        (command,)
    )
    job_id = str(cur.fetchone()[0])
    cur.close()
    conn.close()
    return job_id


def update_job_assigned(job_id: str, worker_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE jobs SET status = 'assigned', assigned_to = %s WHERE job_id = %s;",
        (worker_id, job_id)
    )
    cur.close()
    conn.close()


def update_job_running(job_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE jobs SET status = 'running', started_at = NOW() WHERE job_id = %s;",
        (job_id,)
    )
    cur.close()
    conn.close()


def update_job_completed(job_id: str, output: str, exit_code: int):
    status = "completed" if exit_code == 0 else "failed"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE jobs SET status = %s, output = %s, exit_code = %s, completed_at = NOW() WHERE job_id = %s;",
        (status, output, exit_code, job_id)
    )
    cur.close()
    conn.close()


def get_job(job_id: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM jobs WHERE job_id = %s;", (job_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None


def get_all_jobs() -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM jobs ORDER BY created_at DESC;")
    rows = [dict(row) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def get_jobs_for_worker(worker_id: str, statuses: list[str]) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT * FROM jobs WHERE assigned_to = %s AND status = ANY(%s);",
        (worker_id, statuses)
    )
    rows = [dict(row) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def mark_job_pending(job_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE jobs SET status = 'pending', assigned_to = NULL, started_at = NULL WHERE job_id = %s;",
        (job_id,)
    )
    cur.close()
    conn.close()
