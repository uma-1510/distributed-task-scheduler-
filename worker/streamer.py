 # Streams job output back line by line

import subprocess
import sys
sys.path.insert(0, '.')

from proto import scheduler_pb2


def stream_job_output(request, context):
    """
    Generator that streams stdout line by line as the job runs.
    Used by the StreamJobOutput gRPC endpoint.
    """
    proc = subprocess.Popen(
        request.command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    seq = 0
    for line in iter(proc.stdout.readline, ''):
        if context.is_active():
            yield scheduler_pb2.OutputChunk(
                job_id=request.job_id,
                line=line.rstrip(),
                seq_num=seq
            )
            seq += 1
        else:
            proc.terminate()
            return

    proc.wait()