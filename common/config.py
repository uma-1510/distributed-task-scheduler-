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


