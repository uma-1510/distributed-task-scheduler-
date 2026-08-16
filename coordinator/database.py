"""PostgreSQL access layer for the coordinator.

All queries go through the module-level connection pool via get_conn()
(see issue #3) instead of opening/closing a raw connection per call. Pool
size is tuned with DB_POOL_MIN_CONN / DB_POOL_MAX_CONN in common/config.py.
"""

import threading
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import psycopg2.pool
from common.config import DATABASE_URL, DB_POOL_MIN_CONN, DB_POOL_MAX_CONN
from common.models import JobStatus, WorkerStatus

# A module-level pool, created lazily on first use. Every DB function in this
# file used to call psycopg2.connect()/close() per query — at 3+ workers
# heartbeating every 5s plus job traffic, that's dozens of fresh TCP +
# Postgres-auth handshakes per second for no reason. The pool keeps a small
# set of live connections open and hands them out/back instead.
#
# ThreadedConnectionPool, not SimpleConnectionPool: this app is genuinely
# multi-threaded — the heartbeat monitor runs on its own thread, FastAPI's
# sync route handlers each run on their own thread from FastAPI's own pool
# — and psycopg2's docs are explicit that SimpleConnectionPool is for
# single-threaded use only. _pool_lock guards the lazy-init check-then-set
# so two threads racing to initialize don't each create their own pool,
# leaving one of them orphaned and unreachable to close_pool().
_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:  # re-check: another thread may have won the race
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    DB_POOL_MIN_CONN, DB_POOL_MAX_CONN, DATABASE_URL
                )
    return _pool


@contextmanager
def get_conn():
    """Check out a pooled connection, auto-return it when the block exits
    (including on exception) instead of every call site managing its own
    connect()/close()."""
    pool = _get_pool()
    conn = pool.getconn()
    conn.autocommit = True
    try:
        yield conn
    finally:
        pool.putconn(conn)


def close_pool():
    """Call on coordinator shutdown so connections don't leak past process
    lifetime (mainly matters for tests that spin up/tear down repeatedly)."""
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.closeall()
            _pool = None


def get_connection():
    """Kept for any external callers, but no longer used internally in this
    file — prefer the `get_conn()` context manager, which returns the
    connection to the pool instead of closing it."""
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def create_tables():
    with get_conn() as conn:
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
    print("[db] tables ready")


def reset_all_workers():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE workers SET status = 'dead', last_heartbeat = NOW(), registered_at = NOW();")
        cur.close()
    print("[db] all workers reset — timestamps refreshed")


def reset_worker_heartbeats():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE workers
            SET last_heartbeat = NOW(),
                status = 'alive'
        """)
        cur.close()
    print("[db] reset all worker heartbeats on startup")

def upsert_worker(worker_id: str, address: str, port: int):
    with get_conn() as conn:
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


def update_worker_heartbeat(worker_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE workers SET last_heartbeat = NOW(), status = 'healthy' WHERE worker_id = %s;",
            (worker_id,)
        )
        cur.close()


def update_worker_status(worker_id: str, status: WorkerStatus):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE workers SET status = %s WHERE worker_id = %s;",
            (status.value, worker_id)
        )
        cur.close()


def update_workers_status_batch(worker_ids: list[str], status: WorkerStatus):
    """Mark multiple workers' status in a single UPDATE instead of one
    round-trip per worker — see issue #6. No-op on an empty list so callers
    don't need to guard against it."""
    if not worker_ids:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE workers SET status = %s WHERE worker_id = ANY(%s);",
                (status.value, worker_ids)
            )
        finally:
            cur.close()
    finally:
        conn.close()


def get_all_workers() -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT worker_id, address, port, status, last_heartbeat, registered_at FROM workers;")
        rows = [dict(row) for row in cur.fetchall()]
        cur.close()
    return rows


def get_healthy_workers() -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT worker_id, address, port, status, last_heartbeat, registered_at FROM workers WHERE status = 'healthy';")
        rows = [dict(row) for row in cur.fetchall()]
        cur.close()
    return rows


def create_job(command: str) -> str:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO jobs (command, status) VALUES (%s, 'pending') RETURNING job_id;",
            (command,)
        )
        job_id = str(cur.fetchone()[0])
        cur.close()
    return job_id


def update_job_assigned(job_id: str, worker_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET status = 'assigned', assigned_to = %s WHERE job_id = %s;",
            (worker_id, job_id)
        )
        cur.close()


def update_job_running(job_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET status = 'running', started_at = NOW() WHERE job_id = %s;",
            (job_id,)
        )
        cur.close()


def update_job_completed(job_id: str, output: str, exit_code: int):
    status = "completed" if exit_code == 0 else "failed"
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET status = %s, output = %s, exit_code = %s, completed_at = NOW() WHERE job_id = %s;",
            (status, output, exit_code, job_id)
        )
        cur.close()


def get_job(job_id: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM jobs WHERE job_id = %s;", (job_id,))
        row = cur.fetchone()
        cur.close()
    return dict(row) if row else None


def get_all_jobs() -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM jobs ORDER BY created_at DESC;")
        rows = [dict(row) for row in cur.fetchall()]
        cur.close()
    return rows


def get_jobs_for_worker(worker_id: str, statuses: list[str]) -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM jobs WHERE assigned_to = %s AND status = ANY(%s);",
            (worker_id, statuses)
        )
        rows = [dict(row) for row in cur.fetchall()]
        cur.close()
    return rows


def mark_job_pending(job_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET status = 'pending', assigned_to = NULL, started_at = NULL WHERE job_id = %s;",
            (job_id,)
        )
        cur.close()


def mark_jobs_pending_batch(job_ids: list[str]):
    """Same as mark_job_pending, but for a whole batch in a single UPDATE —
    used when reassigning every job a dead worker was holding at once
    instead of one round-trip per job. See issue #7. No-op on empty list."""
    if not job_ids:
        return
    with get_conn() as conn:
        cur = conn.cursor()
        # job_id is a UUID column; psycopg2 adapts a Python list of str to a
        # text[] literal, and Postgres won't implicitly compare that against
        # uuid — needs an explicit cast, or this fails at runtime with
        # "operator does not exist: uuid = text" (caught via live testing
        # on this exact bug the first time this function was written).
        cur.execute(
            "UPDATE jobs SET status = 'pending', assigned_to = NULL, started_at = NULL WHERE job_id = ANY(%s::uuid[]);",
            (job_ids,)
        )
        cur.close()
