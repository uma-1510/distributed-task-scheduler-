# gRPC server, registers with coordinator

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


def send_heartbeats(worker_id: str):
    url = f"http://{COORDINATOR_HOST}:{COORDINATOR_PORT}/workers/{worker_id}/heartbeat"
    while True:
        try:
            requests.post(url, timeout=3)
            print(f"[heartbeat] ping sent — {worker_id}")
        except Exception as e:
            print(f"[heartbeat] failed to reach coordinator: {e}")
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