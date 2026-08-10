# gRPC server, registers with coordinator
#
# Heartbeat sends run on a dedicated single-thread executor so a slow or
# unreachable coordinator can't skew the 5s ping cadence — see issue #2.

import grpc
import sys
import os
import threading
import requests
import time
from concurrent import futures

sys.path.insert(0, '.')

from proto import scheduler_pb2, scheduler_pb2_grpc
from worker.executor import execute_job
from common.config import COORDINATOR_HOST, COORDINATOR_PORT, HEARTBEAT_INTERVAL

from worker.streamer import stream_job_output

class WorkerService(scheduler_pb2_grpc.WorkerServiceServicer):

    def AssignJob(self, request, context):
        print(f"[worker] received job {request.job_id}: {request.command}")
        thread = threading.Thread(
            target=execute_job,
            args=(request.job_id, request.command),
            daemon=True
        )
        thread.start()
        return scheduler_pb2.JobResponse(job_id=request.job_id, status="accepted")

    def StreamJobOutput(self, request, context):
        print(f"[worker] streaming output for job {request.job_id}")
        yield from stream_job_output(request, context)

    def Heartbeat(self, request, context):
        return scheduler_pb2.HeartbeatResponse(acknowledged=True)


# Sending each heartbeat used to block send_heartbeats()'s own loop thread
# on requests.post() — if the coordinator was slow, the *next* heartbeat
# would fire late too, since time.sleep(HEARTBEAT_INTERVAL) only ran after
# the blocking call returned. A worker could get marked DEAD purely because
# the coordinator was busy, not because the worker itself was unhealthy.
# Sending on a dedicated one-thread executor instead means the loop always
# sleeps exactly HEARTBEAT_INTERVAL regardless of how long the coordinator
# takes to respond. See issue #2.
heartbeat_executor = futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="heartbeat-sender")

# Now that a slow send can't delay the next one, there's no reason to keep
# the old tight 3s request timeout — give the coordinator more room before
# giving up on a single ping.
HEARTBEAT_SEND_TIMEOUT_SECONDS = 10

# If pings are backing up in the executor's queue faster than they're being
# sent, that's a real signal the coordinator's been unreachable for a while
# — worth a log even though nothing here can act on it directly (the
# heartbeat monitor on the coordinator side is what actually decides this
# worker is dead).
HEARTBEAT_QUEUE_WARNING_THRESHOLD = 5

# Hard cap on how many pings can queue up. Without this, a coordinator
# that's unreachable for hours/days would mean this worker submits one new
# task every 5s forever, growing the queue without bound. Once we're at the
# cap, skip submitting new pings until the backlog drains — the queued ones
# will still fire in order, just later, and one fresh ping the moment the
# coordinator comes back matters more than N stale ones queued behind it.
HEARTBEAT_QUEUE_MAX_DEPTH = 100


def _send_one_heartbeat(worker_id: str):
    url = f"http://{COORDINATOR_HOST}:{COORDINATOR_PORT}/workers/{worker_id}/heartbeat"
    try:
        requests.post(url, timeout=HEARTBEAT_SEND_TIMEOUT_SECONDS)
        print(f"[heartbeat] ping sent — {worker_id}")
    except Exception as e:
        print(f"[heartbeat] failed to reach coordinator: {e}")


def send_heartbeats(worker_id: str):
    while True:
        queue_depth = heartbeat_executor._work_queue.qsize()
        if queue_depth > HEARTBEAT_QUEUE_WARNING_THRESHOLD:
            print(f"[heartbeat] ⚠️  {queue_depth} pings backed up — coordinator may be unreachable")

        if queue_depth < HEARTBEAT_QUEUE_MAX_DEPTH:
            heartbeat_executor.submit(_send_one_heartbeat, worker_id)
        else:
            print(f"[heartbeat] queue at max depth ({HEARTBEAT_QUEUE_MAX_DEPTH}) — skipping this ping")

        time.sleep(HEARTBEAT_INTERVAL)


def register_with_coordinator(worker_id: str, address: str, port: int):
    url = f"http://{COORDINATOR_HOST}:{COORDINATOR_PORT}/workers/register"
    for attempt in range(10):
        try:
            r = requests.post(url, json={
                "worker_id": worker_id,
                "address":   address,
                "port":      port
            }, timeout=5)
            r.raise_for_status()
            print(f"[worker] registered as {worker_id}")
            return
        except Exception as e:
            print(f"[worker] registration attempt {attempt+1} failed: {e}")
            time.sleep(3)
    raise RuntimeError("Could not register with coordinator after 10 attempts")


def serve(worker_id: str, port: int):
    # In Docker, workers register with their service name as address
    # so coordinator can reach them via Docker DNS
    address = os.getenv("WORKER_ADDRESS", "localhost")
    register_with_coordinator(worker_id, address, port)

    hb_thread = threading.Thread(
        target=send_heartbeats,
        args=(worker_id,),
        daemon=True
    )
    hb_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    scheduler_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[worker] {worker_id} listening on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    worker_id = os.getenv("WORKER_ID", "worker-1")
    port      = int(os.getenv("GRPC_PORT", "50051"))
    serve(worker_id, port)