import pytest
from src.distributed.quorum_simulator import QuorumCluster, QuorumError


def test_quorum_cluster_parameter_validation():
    """Verifies validation of replication factor (rf), write quorum (w), and read quorum (r)."""
    nodes = ["node-a", "node-b", "node-c"]

    with pytest.raises(ValueError):
        QuorumCluster(nodes=nodes, rf=0)

    with pytest.raises(ValueError):
        QuorumCluster(nodes=nodes, rf=4)  # rf cannot exceed total nodes

    cluster = QuorumCluster(nodes=nodes, rf=3)

    with pytest.raises(ValueError):
        cluster.write("k", "v", w=4)

    with pytest.raises(ValueError):
        cluster.read("k", r=0)

    with pytest.raises(KeyError):
        cluster.partition_node("unknown-node")

    with pytest.raises(KeyError):
        cluster.heal_node("unknown-node")


def test_strong_consistency_quorum_overlap_under_lag():
    """
    Proves W + R > RF (2 + 2 > 3) guarantees reading the latest write even when
    asynchronous replication lags (simulate_lag=True writes only to the first W nodes,
    while read samples the last R nodes).
    """
    cluster = QuorumCluster(nodes=["node-1", "node-2", "node-3"], rf=3, node_id=1)

    # First write fully propagates to all 3 replicas
    v1_id = cluster.write("post:10:pinned", "old-comment", w=2, simulate_lag=False)

    # Second write experiences replication lag (only reaches first W=2 replicas)
    v2_id = cluster.write("post:10:pinned", "new-comment", w=2, simulate_lag=True)

    assert v2_id > v1_id  # Monotonic Snowflake version_id
    # Reading with R=2 forces an overlap on the middle replica and picks max(version_id)
    assert cluster.read("post:10:pinned", r=2) == "new-comment"


def test_eventual_consistency_stale_read_when_w_plus_r_le_rf():
    """
    Proves W + R <= RF (1 + 1 <= 3) permits stale reads during replication lag:
    write(w=1, simulate_lag=True) updates healthy_nodes[:1], while read(r=1) reads healthy_replicas[-1:].
    """
    cluster = QuorumCluster(nodes=["node-1", "node-2", "node-3"], rf=3, node_id=2)

    # Seed initial value across all 3 replicas
    cluster.write("user:5:bio", "v1-initial", w=3, simulate_lag=False)

    # Fast AP write (W=1) with replication lag only updates replica #0
    cluster.write("user:5:bio", "v2-updated", w=1, simulate_lag=True)

    # Fast AP read (R=1) hits replica #2, returning the stale v1 value
    stale_val = cluster.read("user:5:bio", r=1)
    assert stale_val == "v1-initial"

    # Upgrading read to R=3 intersects replica #0 and resolves to v2-updated via LWW
    strong_val = cluster.read("user:5:bio", r=3)
    assert strong_val == "v2-updated"


def test_cap_theorem_network_partitions_and_healing():
    """
    Tests CP vs AP behavior during network partitions:
    - With RF=3, W=2, R=2: survives 1 downed node, fails with QuorumError when 2 nodes go down (CP).
    - With RF=3, W=1, R=1: continues serving writes and reads even with 2 downed nodes (AP).
    """
    nodes = ["node-a", "node-b", "node-c"]
    cluster = QuorumCluster(nodes=nodes, rf=3, node_id=3)

    # 1. Partition 1 node -> QUORUM (W=2, R=2) still succeeds
    cluster.partition_node("node-a")
    cluster.write("account:99:status", "active", w=2)
    assert cluster.read("account:99:status", r=2) == "active"

    # 2. Partition a 2nd node -> QUORUM (W=2, R=2) sacrifices Availability for Consistency (CP)
    cluster.partition_node("node-b")
    with pytest.raises(QuorumError):
        cluster.write("account:99:status", "suspended", w=2)
    with pytest.raises(QuorumError):
        cluster.read("account:99:status", r=2)

    # 3. AP mode (W=1, R=1) stays available on the single surviving node ("node-c")
    cluster.write("account:99:status", "degraded-ap-write", w=1)
    assert cluster.read("account:99:status", r=1) == "degraded-ap-write"

    # 4. Heal partitioned nodes -> QUORUM (R=2) is restored and reconciles latest LWW state
    cluster.heal_node("node-a")
    cluster.heal_node("node-b")
    assert cluster.read("account:99:status", r=2) == "degraded-ap-write"