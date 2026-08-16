# Loads config from env variables

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://scheduler_user:scheduler_pass@localhost:5432/scheduler_db")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
COORDINATOR_HOST = os.getenv("COORDINATOR_HOST", "localhost")
COORDINATOR_PORT = int(os.getenv("COORDINATOR_PORT", "8000"))
HEARTBEAT_INTERVAL  = 5
HEARTBEAT_TIMEOUT   = 15
VIRTUAL_NODES       = 150
WORKER_STARTUP_GRACE = 20  # seconds

# Connection pool sizing for coordinator/database.py. min=2 keeps a couple of
# connections warm; max=20 caps how many the coordinator can hold open at
# once regardless of load (see issue #3).
DB_POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN_CONN", "2"))
DB_POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX_CONN", "20"))

# Bad values here (e.g. min > max, or max <= 0) would otherwise surface as a
# confusing PoolError deep inside psycopg2 the first time a query runs.
# Fail fast at import time instead, with a message that says what's wrong.
if not (0 <= DB_POOL_MIN_CONN <= DB_POOL_MAX_CONN) or DB_POOL_MAX_CONN < 1:
    raise ValueError(
        f"invalid DB pool bounds: DB_POOL_MIN_CONN={DB_POOL_MIN_CONN}, "
        f"DB_POOL_MAX_CONN={DB_POOL_MAX_CONN} — require "
        f"0 <= DB_POOL_MIN_CONN <= DB_POOL_MAX_CONN and DB_POOL_MAX_CONN >= 1"
    )

# Caps how many jobs a single worker will execute concurrently. Previously
# unbounded — AssignJob() spawned a raw thread per job with no limit. See
# issue #9. Tune up for I/O-bound jobs, down for CPU-bound ones.
WORKER_MAX_THREADS = int(os.getenv("WORKER_MAX_THREADS", "10"))
if WORKER_MAX_THREADS < 1:
    # ThreadPoolExecutor(max_workers=0 or negative) raises ValueError anyway,
    # but from deep inside the executor construction — fail here instead
    # with a message that actually says what's wrong.
    raise ValueError(f"WORKER_MAX_THREADS must be at least 1, got {WORKER_MAX_THREADS}")


