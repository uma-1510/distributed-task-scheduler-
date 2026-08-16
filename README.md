# Distributed Task Scheduler

A distributed job scheduler built from scratch in Python. A FastAPI coordinator routes submitted jobs to worker nodes over gRPC using consistent hashing, detects worker failures via heartbeats, and reassigns their in-flight jobs automatically.

![architecture diagram](https://github.com/user-attachments/assets/2044a772-6aeb-4084-a239-cf530ceb9de6)

## Requirements

Docker and Docker Compose — that's it. Everything else (Python 3.12, Postgres, Redis) runs inside the containers `docker compose up --build` starts. Running tests or scripts outside Docker additionally needs Python 3.12+ and `pip install -r requirements.txt`.

## Quick start

```bash
docker compose up --build
```

Starts PostgreSQL, Redis, the coordinator (`:8000`), 3 workers (`:50051`–`:50053`), and the dashboard (`:5173`).

```bash
# submit a job
curl -X POST localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"command": "echo hello"}'

# check job status
curl localhost:8000/jobs

# check cluster health
curl localhost:8000/workers

# simulate a failure — kill a worker mid-job, watch it get reassigned within 15s
docker compose stop worker-2
```

## How it works

- **Job routing** — consistent hashing with 150 virtual nodes per worker. Adding or removing a worker remaps only ~1/N of jobs, versus ~(N−1)/N for `job_id % N`.
- **Failure detection** — each worker heartbeats every 5s. 3 missed heartbeats (15s) marks it dead, removes it from the hash ring, and reassigns its in-flight jobs to a healthy worker.
- **Internal comms** — gRPC + Protobuf between coordinator and workers (contract in [`proto/scheduler.proto`](proto/scheduler.proto)); REST for the public API.
- **Job execution** — workers run jobs as subprocesses in background threads; stdout and exit code persist to PostgreSQL.

## API

| Method | Endpoint | Description |
|---|---|---|
| POST | `/jobs` | Submit a job |
| GET | `/jobs?skip=&limit=&status=&since=` | List jobs — paginated (`skip`/`limit`, default `limit=200`) and optionally filtered by `status` or `since` (ISO timestamp) |
| GET | `/jobs/{id}` | Job status + output |
| GET | `/workers?skip=&limit=` | Worker status — paginated (default `limit=100`) |
| GET | `/debug/ring` | Inspect hash ring state |
| POST | `/workers/register`, `/workers/{id}/heartbeat` | Internal — used by workers, not clients |

Full interactive reference at `localhost:8000/docs` (FastAPI's built-in Swagger UI) once the stack is running.

## Chaos testing

[`chaos_test.py`](chaos_test.py) automates the manual failure test above: kill a worker mid-job, repeatedly, and confirm the coordinator reassigns it.

```bash
python3 chaos_test.py --runs 50 --workers worker-1,worker-2,worker-3
python3 chaos_test_summary.py chaos_test_results.json
```

Latest committed run ([`chaos_test_results.json`](chaos_test_results.json)): **19/19 real kill-and-reassign attempts passed (100%)**, ~21.7s average reassignment latency across 50 runs.

## Testing

```bash
python3 tests/test_hash_ring.py         # consistent hashing (7 tests)
python3 tests/test_chaos_summary.py     # chaos results parsing
python3 tests/test_worker_heartbeats.py # heartbeat decoupling
```

## Key design decisions

- **Consistent hashing over `% N`** — bounded remapping when the cluster changes size; same approach Kafka and DynamoDB use for partition/key routing.
- **gRPC over REST internally** — typed contract via `.proto`, binary serialization, native streaming (used for live job output).
- **150 virtual nodes per worker** — without them, ring placement clusters unevenly across a small number of workers.
- **15s heartbeat timeout** — 3× a 5s interval; short enough to detect failures quickly, long enough to avoid false positives from transient latency.

## Tech stack

Python · FastAPI · gRPC/Protobuf · PostgreSQL · Docker Compose


## Contributing

Branch naming, commit style, and the PR workflow this repo uses are in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
