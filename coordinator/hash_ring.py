# Consistent hashing implementation
#
# _hash() uses xxHash (64-bit output) rather than MD5 (128-bit) — see issue
# #11. Trades hash space (2^64 vs 2^128) for speed; 2^64 is still far more
# than enough to avoid meaningful collision risk at any vnode count this
# system would realistically run (150 vnodes/worker × dozens of workers is
# nowhere near where a 64-bit space's birthday bound would start to matter).

import xxhash
from sortedcontainers import SortedDict
from typing import Dict, Optional, List


class ConsistentHashRing:
    """
    Consistent Hash Ring using virtual nodes.

    Time Complexity:
        add_worker      -> O(V log N)
        remove_worker   -> O(V log N)
        get_worker      -> O(log N)   ✅ optimized
    """

    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes

        # hash_position -> worker_id
        self.ring: SortedDict[int, str] = SortedDict()

        # worker_id -> metadata
        self.workers: Dict[str, dict] = {}

    # Internal Hash Function
    def _hash(self, key: str) -> int:
        """Returns a deterministic hash. Uses xxHash (xxh64) instead of MD5
        — MD5 is cryptographically overkill for ring placement, where all
        that's needed is a fast, well-distributed non-cryptographic hash.
        See issue #11."""
        return xxhash.xxh64(key.encode("utf-8")).intdigest()

    # Worker Management
    def add_worker(self, worker_id: str, metadata: Optional[dict] = None):
        """Add worker with virtual nodes."""
        if worker_id in self.workers:
            self.remove_worker(worker_id)

        self.workers[worker_id] = metadata or {}

        for i in range(self.virtual_nodes):
            vnode_key = f"{worker_id}:vnode:{i}"
            position = self._hash(vnode_key)
            self.ring[position] = worker_id

        print(f"[ring] added {worker_id} — ring size: {len(self.ring)} vnodes")

    def remove_worker(self, worker_id: str):
        """Remove worker and all its virtual nodes."""
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

    # Utilities
    def get_all_workers(self) -> List[str]:
        return list(self.workers.keys())

    def get_ring_distribution(self) -> Dict[str, int]:
        """Return vnode ownership count per worker."""
        distribution = {w: 0 for w in self.workers}

        for worker_id in self.ring.values():
            distribution[worker_id] += 1

        return distribution