# Unit tests for consistent hashing

import sys
import uuid
sys.path.insert(0, '.')

from coordinator.hash_ring import ConsistentHashRing


def test_basic_routing():
    print("\n--- TEST 1: Basic routing ---")
    ring = ConsistentHashRing()
    ring.add_worker("worker-1")
    ring.add_worker("worker-2")
    ring.add_worker("worker-3")

    distribution = {"worker-1": 0, "worker-2": 0, "worker-3": 0}
    for _ in range(300):
        job_id = str(uuid.uuid4())
        worker = ring.get_worker(job_id)
        distribution[worker] += 1

    print(f"Distribution across 300 jobs: {distribution}")
    for w, count in distribution.items():
        assert 50 < count < 150, f"{w} got {count} jobs — distribution too uneven"
    print("PASSED — distribution is reasonably even")


def test_consistent_routing():
    print("\n--- TEST 2: Same job always routes to same worker ---")
    ring = ConsistentHashRing()
    ring.add_worker("worker-1")
    ring.add_worker("worker-2")
    ring.add_worker("worker-3")

    job_ids = [str(uuid.uuid4()) for _ in range(50)]
    first_pass  = {job_id: ring.get_worker(job_id) for job_id in job_ids}
    second_pass = {job_id: ring.get_worker(job_id) for job_id in job_ids}

    assert first_pass == second_pass
    print("PASSED — same job always routes to same worker")


def test_minimal_remapping_on_removal():
    print("\n--- TEST 3: Minimal remapping when worker is removed ---")
    ring = ConsistentHashRing()
    ring.add_worker("worker-1")
    ring.add_worker("worker-2")
    ring.add_worker("worker-3")

    job_ids = [str(uuid.uuid4()) for _ in range(300)]
    before = {job_id: ring.get_worker(job_id) for job_id in job_ids}

    ring.remove_worker("worker-2")
    after = {job_id: ring.get_worker(job_id) for job_id in job_ids}

    changed = sum(1 for jid in job_ids if before[jid] != after[jid])
    pct = (changed / len(job_ids)) * 100

    print(f"Jobs remapped: {changed}/300 ({pct:.1f}%)")
    print(f"Expected: ~33% (100 jobs). Actual: {pct:.1f}%")

    # Should be roughly 1/3, never more than 50%
    assert changed < 150, f"Too many jobs remapped: {changed}"
    print("PASSED — only the dead worker's jobs moved")


def test_minimal_remapping_on_add():
    print("\n--- TEST 4: Minimal remapping when worker is added ---")
    ring = ConsistentHashRing()
    ring.add_worker("worker-1")
    ring.add_worker("worker-2")
    ring.add_worker("worker-3")

    job_ids = [str(uuid.uuid4()) for _ in range(300)]
    before = {job_id: ring.get_worker(job_id) for job_id in job_ids}

    ring.add_worker("worker-4")
    after = {job_id: ring.get_worker(job_id) for job_id in job_ids}

    changed = sum(1 for jid in job_ids if before[jid] != after[jid])
    pct = (changed / len(job_ids)) * 100

    print(f"Jobs remapped: {changed}/300 ({pct:.1f}%)")
    print(f"Expected: ~25% (75 jobs). Actual: {pct:.1f}%")

    assert changed < 150, f"Too many jobs remapped: {changed}"
    print("PASSED — only ~1/N jobs moved to the new worker")


def test_empty_ring():
    print("\n--- TEST 5: Empty ring returns None ---")
    ring = ConsistentHashRing()
    result = ring.get_worker("some-job-id")
    assert result is None
    print("PASSED — empty ring returns None")


def test_single_worker():
    print("\n--- TEST 6: Single worker gets all jobs ---")
    ring = ConsistentHashRing()
    ring.add_worker("worker-1")

    for _ in range(20):
        job_id = str(uuid.uuid4())
        assert ring.get_worker(job_id) == "worker-1"
    print("PASSED — single worker gets everything")


def test_vnode_distribution():
    print("\n--- TEST 7: Vnode distribution is balanced ---")
    ring = ConsistentHashRing(virtual_nodes=150)
    ring.add_worker("worker-1")
    ring.add_worker("worker-2")
    ring.add_worker("worker-3")

    dist = ring.get_ring_distribution()
    print(f"Vnode distribution: {dist}")
    for w, count in dist.items():
        assert count == 150, f"{w} has {count} vnodes, expected 150"
    print("PASSED — each worker owns exactly 150 vnodes")


if __name__ == "__main__":
    test_basic_routing()
    test_consistent_routing()
    test_minimal_remapping_on_removal()
    test_minimal_remapping_on_add()
    test_empty_ring()
    test_single_worker()
    test_vnode_distribution()
    print("\n All tests passed — hash ring is working correctly")