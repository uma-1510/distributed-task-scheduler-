# Consistent hashing implementation

import hashlib
import threading
from sortedcontainers import SortedDict
from typing import Dict, Optional, List, Tuple


class ConsistentHashRing:
    """
    Consistent Hash Ring using virtual nodes.

    Time Complexity:
        add_worker      -> O(V log N)
        remove_worker   -> O(V log N)
        get_worker      -> O(log N)   ✅ optimized

    Thread safety: HeartbeatMonitor (its own thread), worker registration,
    and job routing (each FastAPI request's own thread) can all mutate or
    read this ring concurrently. All public methods take self._lock, so a
    remove_worker() from one thread can't interleave with another thread's
    get_worker()/get_worker_and_metadata() call.
    """

    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self._lock = threading.RLock()

        # hash_position -> worker_id
        self.ring: SortedDict[int, str] = SortedDict()

        # worker_id -> metadata
        self.workers: Dict[str, dict] = {}

    # Internal Hash Function
    def _hash(self, key: str) -> int:
        """Returns a deterministic hash in MD5 space."""
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    # Worker Management
    def add_worker(self, worker_id: str, metadata: Optional[dict] = None):
        """Add worker with virtual nodes."""
        with self._lock:
            if worker_id in self.workers:
                self._remove_worker_locked(worker_id)

            self.workers[worker_id] = metadata or {}

            for i in range(self.virtual_nodes):
                vnode_key = f"{worker_id}:vnode:{i}"
                position = self._hash(vnode_key)
                self.ring[position] = worker_id

            print(f"[ring] added {worker_id} — ring size: {len(self.ring)} vnodes")

    def remove_worker(self, worker_id: str):
        """Remove worker and all its virtual nodes."""
        with self._lock:
            self._remove_worker_locked(worker_id)

    def _remove_worker_locked(self, worker_id: str):
        """Same as remove_worker(), assuming self._lock is already held —
        used internally by add_worker() to avoid double-acquiring an RLock
        for a re-registration (harmless with RLock, but keeps the intent
        explicit)."""
        if worker_id not in self.workers:
            return

        for i in range(self.virtual_nodes):
            vnode_key = f"{worker_id}:vnode:{i}"
            position = self._hash(vnode_key)
            self.ring.pop(position, None)

        del self.workers[worker_id]

        print(f"[ring] removed {worker_id} — ring size: {len(self.ring)} vnodes")

    # O(log N) Worker Lookup
    def get_worker(self, job_id: str) -> Optional[str]:
        """
        Returns worker responsible for job_id.

        Uses binary search via SortedDict.bisect_left().
        """
        with self._lock:
            return self._get_worker_locked(job_id)

    def _get_worker_locked(self, job_id: str) -> Optional[str]:
        if not self.ring:
            return None

        position = self._hash(job_id)

        # Binary search for insertion point
        idx = self.ring.bisect_left(position)

        # Wrap around if beyond last vnode
        if idx == len(self.ring):
            idx = 0

        # peekitem(idx) -> (hash_position, worker_id)
        _, worker_id = self.ring.peekitem(idx)
        return worker_id

    def get_worker_and_metadata(self, job_id: str) -> Tuple[Optional[str], Optional[dict]]:
        """Selects a worker for job_id and reads its metadata as one atomic
        operation. Without this, a caller doing get_worker() followed by a
        separate self.workers.get(worker_id) has a window where another
        thread's remove_worker() can run in between, making the metadata
        lookup return None for a worker that was valid a moment ago. See
        issue #4's follow-up review."""
        with self._lock:
            worker_id = self._get_worker_locked(job_id)
            if worker_id is None:
                return None, None
            return worker_id, self.workers.get(worker_id)

    # Utilities
    def get_all_workers(self) -> List[str]:
        with self._lock:
            return list(self.workers.keys())

    def get_ring_distribution(self) -> Dict[str, int]:
        """Return vnode ownership count per worker."""
        with self._lock:
            distribution = {w: 0 for w in self.workers}

            for worker_id in self.ring.values():
                distribution[worker_id] += 1

            return distribution