import threading
import pytest
from unittest.mock import patch
from src.distributed.snowflake import SnowflakeGenerator


def test_node_id_validation():
    """Verify node_id boundaries (0 to 1023) and type checks."""
    # Valid boundary values
    gen_min = SnowflakeGenerator(0)
    gen_max = SnowflakeGenerator(1023)
    assert gen_min.node_id == 0
    assert gen_max.node_id == 1023

    # Out-of-bounds values must raise ValueError
    with pytest.raises(ValueError):
        SnowflakeGenerator(-1)
    with pytest.raises(ValueError):
        SnowflakeGenerator(1024)

    # Non-integer types must raise TypeError
    with pytest.raises(TypeError):
        SnowflakeGenerator("42")  # type: ignore


def test_encode_and_decode_roundtrip():
    """Verify that node_id, sequence, and timestamp survive multiple calls without mutating state."""
    node_id = 42
    gen = SnowflakeGenerator(node_id=node_id)

    # Generate multiple IDs to ensure self.node_id is not mutated in-place across calls
    first_id = gen.next_id()
    second_id = gen.next_id()

    decoded_first = SnowflakeGenerator.decode(first_id)
    decoded_second = SnowflakeGenerator.decode(second_id)

    assert decoded_first["Node_ID"] == node_id
    assert decoded_second["Node_ID"] == node_id
    assert decoded_first["Timestamp"] >= SnowflakeGenerator.EPOCH
    assert second_id > first_id


def test_burst_uniqueness_and_monotonic_sorting():
    """
    Generate 5,000 IDs in a tight loop.
    Because MAX_SEQUENCE is 4,095 per millisecond, 5,000 IDs guarantees
    that the sequence overflow spin-wait loop is triggered and tested.
    """
    gen = SnowflakeGenerator(node_id=7)
    count = 100_000
    ids = [gen.next_id() for _ in range(count)]

    # 1. All 5,000 IDs must be 100% unique
    assert len(set(ids)) == count

    # 2. All IDs must be strictly positive 64-bit integers
    assert all(i > 0 for i in ids)

    # 3. Every ID must be strictly greater than the previous one (chronologically sortable)
    changed = 1

    for i in range(1, count):
        assert ids[i] > ids[i - 1], f"ID at index {i} was not strictly increasing!"
        
def test_clock_drift_raises_runtime_error():
    """Simulate an NTP clock rollback and verify RuntimeError is raised."""
    gen = SnowflakeGenerator(node_id=1)

    with patch.object(SnowflakeGenerator, "_current_ms", side_effect=[1704067205000, 1704067204999]):
        gen.next_id()  # Sets last_timestamp = 1704067205000
        with pytest.raises(RuntimeError, match="Clock moved backwards!"):
            gen.next_id()  # Clock moved back by 1ms -> must fail


def test_multithreaded_concurrency():
    """Verify thread lock prevents duplicate IDs across 8 concurrent worker threads."""
    gen = SnowflakeGenerator(node_id=99)
    num_threads = 8
    ids_per_thread = 500
    results: list[list[int]] = [[] for _ in range(num_threads)]

    def worker(thread_idx: int):
        results[thread_idx] = [gen.next_id() for _ in range(ids_per_thread)]

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    all_ids = [sid for sublist in results for sid in sublist]
    assert len(all_ids) == num_threads * ids_per_thread
    assert len(set(all_ids)) == num_threads * ids_per_thread


from collections import Counter
from distributed.consistent_hash import ConsistentHashRing


def test_ring_empty_and_validation():
    """Verify empty ring behavior and invalid virtual_nodes / rf checks."""
    ring = ConsistentHashRing(virtual_nodes=1)
    assert ring.get_node("post:100") is None
    assert ring.get_preference_list("post:100", rf=3) == []

    with pytest.raises(ValueError):
        ConsistentHashRing(virtual_nodes=0)

    ring.add_node("shard-a")
    assert len(ring.sorted_keys) == 1
    assert ring.get_node("post:100") == "shard-a"

    with pytest.raises(ValueError):
        ring.get_preference_list("post:100", rf=0)

    with pytest.raises(KeyError):
        ring.remove_node("non-existent-shard")


def test_preference_list_returns_distinct_physical_nodes():
    """Verify clockwise walk skips duplicate vnodes of the same physical server."""
    nodes = ["shard-1", "shard-2", "shard-3", "shard-4"]
    ring = ConsistentHashRing(nodes=nodes, virtual_nodes=100)

    prefs = ring.get_preference_list("post:999", rf=3)
    assert len(prefs) == 3
    assert len(set(prefs)) == 3  # All 3 replicas must be distinct physical nodes
    assert prefs[0] == ring.get_node("post:999")

    # Requesting rf > total physical nodes should cap at len(nodes)
    prefs_all = ring.get_preference_list("post:999", rf=10)
    assert len(prefs_all) == 4


def test_virtual_nodes_load_distribution():
    """Verify 100 virtual nodes per shard distribute 3,000 keys evenly across 3 shards."""
    ring = ConsistentHashRing(nodes=["shard-a", "shard-b", "shard-c"], virtual_nodes=150)
    counts = Counter(ring.get_node(f"comment:{i}") for i in range(3000))

    # Ideal share is 1,000 keys per node. With 150 vnodes, each node should hold between 750 and 1,250.
    for shard in ["shard-a", "shard-b", "shard-c"]:
        assert 750 <= counts[shard] <= 1250, f"Unbalanced load on {shard}: {counts[shard]}"


def test_minimal_key_remapping_on_scale_out():
    """
    Prove the core System Design property:
    Scaling from 3 -> 4 shards only remaps ~25% (1/4) of keys, not 75%.
    """
    initial_nodes = ["shard-1", "shard-2", "shard-3"]
    ring = ConsistentHashRing(nodes=initial_nodes, virtual_nodes=100)

    keys = [f"post:{i}" for i in range(2000)]
    before_mapping = {k: ring.get_node(k) for k in keys}

    # Scale horizontally: add 4th shard
    ring.add_node("shard-4")
    after_mapping = {k: ring.get_node(k) for k in keys}

    moved_keys = sum(1 for k in keys if before_mapping[k] != after_mapping[k])
    moved_ratio = moved_keys / len(keys)

    # Theoretical relocation ratio is 1/4 = 0.25 (allow 0.18 to 0.32 variance)
    assert 0.18 <= moved_ratio <= 0.32, f"Expected ~25% keys moved, got {moved_ratio:.2%}"

    # Removing shard-4 must restore 100% of keys to their exact original shards
    ring.remove_node("shard-4")
    restored_mapping = {k: ring.get_node(k) for k in keys}
    assert before_mapping == restored_mapping



import pytest
from src.distributed.quorum_simulator import QuorumCluster, QuorumError


def test_quorum_cluster_initialization():
    """Verify initialization boundaries and validation."""
    cluster = QuorumCluster(nodes=["n1", "n2", "n3", "n4"], rf=3)
    assert cluster.rf == 3
    assert len(cluster.storage) == 4

    with pytest.raises(ValueError):
        QuorumCluster(nodes=["n1", "n2"], rf=5)  # rf > len(nodes)
    with pytest.raises(ValueError):
        QuorumCluster(nodes=["n1", "n2"], rf=0)  # rf < 1


def test_strong_consistency_with_replication_lag():
    """
    Prove W + R > RF Strong Consistency ($CP$):
    With RF=3, W=2, R=2 (4 > 3):
    Even if simulate_lag=True leaves the 3rd replica lagging,
    the read quorum is mathematically guaranteed to overlap with at least one updated node
    and return the latest version via Last-Write-Wins (LWW).
    """
    cluster = QuorumCluster(nodes=["shard-1", "shard-2", "shard-3"], rf=3)

    # Write v1 with W=2, R=2
    v1 = cluster.write("post:100", "Hello World v1", w=2)
    assert cluster.read("post:100", r=2) == "Hello World v1"

    # Write v2 with simulation of replication lag (only written to first W=2 nodes)
    v2 = cluster.write("post:100", "Hello World v2", w=2, simulate_lag=True)

    # Read quorum (R=2) must successfully reconcile and return v2
    assert cluster.read("post:100", r=2) == "Hello World v2"


def test_quorum_failure_on_excessive_partitions():
    """Verify QuorumError is raised when dead nodes prevent meeting W or R."""
    cluster = QuorumCluster(nodes=["n1", "n2", "n3"], rf=3)

    key = "post:200"
    replicas = cluster.ring.get_preference_list(key, rf=3)
    n_first, n_second, n_third = replicas[0], replicas[1], replicas[2]

    # 1. Partition 2 out of the 3 replicas (n_first and n_second) -> 1 healthy left (n_third)
    cluster.partition_node(n_first)
    cluster.partition_node(n_second)

    # Trying to write with W=2 requires 2 healthy nodes, but only 1 is left -> must fail!
    with pytest.raises(QuorumError):
        cluster.write(key, "data", w=2)

    # 2. Heal n_first and n_second, but partition n_third instead -> n_first and n_second are healthy (2 nodes)
    cluster.heal_node(n_first)
    cluster.heal_node(n_second)
    cluster.partition_node(n_third)

    # Now write with W=2 succeeds because n_first and n_second are healthy!
    cluster.write(key, "data", w=2)
    assert cluster.read(key, r=2) == "data"

    # 3. Partition n_second as well -> only n_first is healthy. R=2 must fail!
    cluster.partition_node(n_second)
    with pytest.raises(QuorumError):
        cluster.read(key, r=2)