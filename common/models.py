# Shared data models (JobStatus enum etc)
from enum import Enum


class JobStatus(str, Enum):
    """A job's lifecycle state, mirrored by the `status` column on the
    `jobs` table. See coordinator/database.py's update_job_* functions for
    the transitions between these."""
    PENDING    = "pending"     # created, not yet routed to a worker
    ASSIGNED   = "assigned"    # routed to a worker, not yet started running
    RUNNING    = "running"     # worker has started executing the command
    COMPLETED  = "completed"   # finished with exit code 0
    FAILED     = "failed"      # finished with a nonzero exit code
    REASSIGNED = "reassigned"  # was on a worker that died; routed elsewhere


class WorkerStatus(str, Enum):
    """A worker's health state as tracked by the coordinator's heartbeat
    monitor (coordinator/heartbeat_monitor.py)."""
    HEALTHY   = "healthy"    # heartbeating within HEARTBEAT_TIMEOUT
    SUSPECTED = "suspected"  # missed at least one heartbeat, not yet dead
    DEAD      = "dead"       # missed HEARTBEAT_TIMEOUT — removed from the ring
    RECOVERED = "recovered"  # re-registered after being marked dead
