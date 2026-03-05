# Distributed Task Scheduler

A production-grade distributed task execution system built from scratch in Python. Jobs submitted via REST API are routed to worker nodes using **consistent hashing**, executed in parallel, and automatically reassigned if a worker fails — with no manual intervention required.

---

## Architecture Diagram
<img width="1143" height="1206" alt="diagram-export-3-2-2026-7_44_18-AM" src="https://github.com/user-attachments/assets/2044a772-6aeb-4084-a239-cf530ceb9de6" />


---

## Demo

---

## How It Works

### Job Routing — Consistent Hashing

When a job is submitted, the coordinator hashes the job ID and maps it to a position on a virtual ring. Workers occupy positions on the ring via 150 virtual nodes each. The job is assigned to the first worker clockwise from its hash position.

**Why consistent hashing over modulo (`job_id % num_workers`)?**

With modulo, adding or removing one worker remaps ~67% of all jobs. With consistent hashing, only ~1/N jobs remap — roughly the dead worker's share. This is how Kafka assigns partitions to brokers and how DynamoDB routes keys to nodes.

```
Before removing worker-2:   300 jobs → ~100 per worker
After removing worker-2:    ~100 jobs remapped, ~200 jobs untouched
With modulo:                ~200 jobs would remap
```

### Failure Detection — Heartbeat Monitor

Every worker sends a heartbeat ping to the coordinator every 5 seconds. A background thread in the coordinator checks timestamps every 5 seconds. If a worker hasn't pinged in 15 seconds (3 missed intervals), it is marked `dead`, removed from the hash ring, and its in-flight jobs are reassigned.

**Worker state machine:**
```
HEALTHY → DEAD → (re-registers as HEALTHY on restart)
```

**Job state machine:**
```
PENDING → ASSIGNED → RUNNING → COMPLETED
                              → FAILED
                   → REASSIGNED (when worker dies)
```

### Internal Communication — gRPC

The coordinator communicates with workers via gRPC with Protobuf serialization. gRPC was chosen over REST for internal comms because it is faster, strongly typed, and supports streaming. The `.proto` file defines the full API contract between services.

Workers also support `StreamJobOutput` — streaming stdout line by line back to the coordinator as the job runs, rather than waiting for completion.

### Job Execution

Workers execute jobs as OS-level subprocesses via Python's `subprocess.Popen`. stdout and stderr are captured and written to PostgreSQL on completion. Each worker runs jobs in a background thread so the gRPC server stays responsive for new assignments.

---

## System Components

```
distributed-task-scheduler/
├── coordinator/
│   ├── main.py               # FastAPI REST API
│   ├── hash_ring.py          # Consistent hashing implementation
│   ├── heartbeat_monitor.py  # Background failure detection thread
│   ├── job_router.py         # Routes jobs to workers via gRPC
│   ├── reassigner.py         # Reassigns dead worker's jobs
│   └── database.py           # PostgreSQL queries
├── worker/
│   ├── main.py               # gRPC server + registration + heartbeat
│   ├── executor.py           # Subprocess job execution
│   └── streamer.py           # Line-by-line stdout streaming
├── proto/
│   └── scheduler.proto       # gRPC service definitions
├── common/
│   ├── models.py             # JobStatus and WorkerStatus enums
│   └── config.py             # Environment config
├── tests/
│   ├── test_hash_ring.py     # 7 unit tests for consistent hashing
│   └── test_failure.py       # End-to-end failure scenario test
├── docker-compose.yml        # Full system in one command
├── Dockerfile.coordinator
└── Dockerfile.worker
```

---

## Database Schema

```sql
CREATE TABLE workers (
    worker_id       VARCHAR PRIMARY KEY,
    address         VARCHAR NOT NULL,
    port            INTEGER NOT NULL,
    status          VARCHAR NOT NULL,   -- healthy / dead
    last_heartbeat  TIMESTAMP,
    registered_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE jobs (
    job_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    command       TEXT NOT NULL,
    status        VARCHAR NOT NULL,     -- pending / assigned / running / completed / failed
    assigned_to   VARCHAR REFERENCES workers(worker_id),
    output        TEXT,
    exit_code     INTEGER,
    created_at    TIMESTAMP DEFAULT NOW(),
    started_at    TIMESTAMP,
    completed_at  TIMESTAMP
);
```

---

## API Reference

### Jobs

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/jobs` | Submit a new job |
| GET | `/jobs` | List all jobs |
| GET | `/jobs/{job_id}` | Get job status and output |

**Submit a job:**
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"command": "echo hello from the scheduler"}'
```

**Response:**
```json
{
  "job_id": "3a4ca6fe-1fb6-4427-9830-122c09fcdcb7",
  "status": "assigned",
  "worker_id": "worker-2"
}
```

### Workers

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/workers` | List all workers and their status |
| POST | `/workers/register` | Register a new worker |
| POST | `/workers/{id}/heartbeat` | Worker heartbeat ping |
| GET | `/debug/ring` | Inspect hash ring state |

---

## Running Locally

### Prerequisites
- Docker Desktop

### Start the full system
```bash
docker compose up --build
```

This starts:
- PostgreSQL (job and worker state)
- Redis (ring state)
- Coordinator (FastAPI on port 8000)
- Worker-1, Worker-2, Worker-3 (gRPC on ports 50051-50053)

### Submit jobs
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"command": "python3 -c \"import time; time.sleep(2); print(42)\""}'
```

### Check job status
```bash
curl http://localhost:8000/jobs
```

### Check cluster health
```bash
curl http://localhost:8000/workers
```

### Simulate worker failure
```bash
# Submit 10 long-running jobs
for i in {1..10}; do
  curl -s -X POST http://localhost:8000/jobs \
    -H "Content-Type: application/json" \
    -d "{\"command\": \"sleep 60 && echo job_$i\"}"
done

# Kill a worker mid-execution
docker compose stop worker-2

# Watch jobs reassign automatically within 15 seconds
curl http://localhost:8000/jobs
curl http://localhost:8000/workers
```

### Run hash ring unit tests
```bash
python tests/test_hash_ring.py
```

**Expected output:**
```
PASSED — distribution is reasonably even
PASSED — same job always routes to same worker
PASSED — only the dead worker's jobs moved  (32.7% remapped, not 67%)
PASSED — only ~1/N jobs moved to the new worker
PASSED — empty ring returns None
PASSED — single worker gets everything
PASSED — each worker owns exactly 150 vnodes

✅ All tests passed — hash ring is working correctly
```

---

## Key Design Decisions

**1. Consistent hashing over modulo**
Modulo remaps ~67% of jobs when a worker is added or removed. Consistent hashing remaps only ~33% — the dead worker's share. This is critical for live systems where job routing must be stable as the cluster changes.

**2. gRPC over REST for internal communication**
REST adds HTTP overhead and loose typing between services. gRPC uses Protobuf binary serialization (faster, smaller payloads) and enforces a typed contract via the `.proto` file. It also supports server-side streaming, which we use for real-time job output.

**3. Virtual nodes for ring balance**
Each worker gets 150 virtual positions on the ring instead of 1. Without virtual nodes, workers cluster unevenly and some handle far more jobs than others. 150 virtual nodes gives even distribution across all workers.

**4. Heartbeat timeout of 15 seconds**
5-second interval × 3 missed = 15-second detection window. Shorter intervals increase network overhead. Longer intervals delay failure detection and leave jobs stuck. 15 seconds is a standard production tradeoff.

**5. PostgreSQL for job state, Redis for ring state**
Jobs need durable, queryable state with foreign key constraints — PostgreSQL. The hash ring is rebuilt from Redis on coordinator restart and doesn't need relational structure — Redis. Separating the two keeps each store doing what it's designed for.

---

## Bugs Encountered and Fixed

**Timezone mismatch (18000 second heartbeat)**
PostgreSQL stored naive timestamps in local time (EST). Python's `datetime.now(timezone.utc)` computed UTC time. The 5-hour EST/UTC offset made every worker appear to have missed 18,000 seconds of heartbeats — permanently dead. Fixed by using `datetime.now()` (naive local time) in the monitor to match the DB timezone consistently.

**gRPC stub import path**
`grpc_tools.protoc` generates stubs that reference each other with bare imports (`import scheduler_pb2`). Inside a Python package (`proto/`), this breaks. Fixed by patching the generated file to use `from proto import scheduler_pb2`.

**In-memory ring lost on coordinator restart**
The hash ring lives in memory. On restart, the ring was empty until workers re-registered. Fixed by rebuilding the ring from the `workers` table on startup, and resetting all worker timestamps to `NOW()` so the heartbeat monitor starts with a clean slate.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| API | Python, FastAPI, Uvicorn |
| Internal comms | gRPC, Protobuf |
| Job execution | Python subprocess |
| State persistence | PostgreSQL |
| Ring state | Redis |
| Containerization | Docker, Docker Compose |
| Testing | Python unittest |
| Dashboard | React (in progress) |

---

## What's Next

- [ ] React dashboard — cluster health, job list, live output streaming
- [ ] Celery integration — replace subprocess with proper task queue
- [ ] Worker auto-scaling — spin up new workers based on queue depth
- [ ] Job retry logic — automatic retry on failure with backoff
- [ ] Metrics — Prometheus + Grafana for latency and throughput
