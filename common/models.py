# Shared data models (JobStatus enum etc)
from enum import Enum

class JobStatus(str, Enum):
    PENDING    = "pending"
    ASSIGNED   = "assigned"
    RUNNING    = "running"
    COMPLETED  = "completed"
    FAILED     = "failed"
    REASSIGNED = "reassigned"

class WorkerStatus(str, Enum):
    HEALTHY   = "healthy"
    SUSPECTED = "suspected"
    DEAD      = "dead"
    RECOVERED = "recovered"