from typing import Any
from src.distributed.consistent_hash import ConsistentHashRing
from src.distributed.snowflake import SnowflakeGenerator

class QuorumError(RuntimeError):
    """Raised when write or read quorums cannot be met due to dead nodes or network partitions."""
    pass

class QuorumCluster:
    def __init__(self, nodes: list[str], rf: int = 3, node_id: int = 0):
        # 1. Validate that 1 <= rf <= len(nodes) (raise ValueError if invalid)
        if not 1 <= rf <= len(nodes):
            raise ValueError
    
        # 2. Store self.rf and initialize self.ring = ConsistentHashRing(nodes=nodes)
        self.rf = rf
        self.ring = ConsistentHashRing(nodes=nodes)
        
        # 3. Initialize self.id_gen = SnowflakeGenerator(node_id=node_id)
        self.id_gen = SnowflakeGenerator(node_id=node_id)

        # 4. Initialize self.storage mapping each node name to an empty dict: {node: {} for node in nodes}
        self.storage = {node : {} for node in nodes}

        # 5. Initialize self.down_nodes: set[str] = set()
        self.down_nodes:set = set()

    def partition_node(self, node: str) -> None:
        if node not in self.storage:
            raise KeyError    
        self.down_nodes.add(node)

    def heal_node(self, node: str) -> None:
        if node not in self.storage:
            raise KeyError
        if node in self.down_nodes:
            self.down_nodes.remove(node)

    def write(self, key: str, value: Any, w: int = 2, simulate_lag: bool = False) -> int:
        if not 1 <= w <= self.rf:
            raise ValueError
        replicas = self.ring.get_preference_list(key, rf=self.rf)

        healthy_nodes = [node for node in replicas if node not in self.down_nodes]
        if len(healthy_nodes) < w:
            raise QuorumError

        version_id = self.id_gen.next_id()
        record = {"value": value, "version_id": version_id}

        target_nodes = healthy_nodes[:w] if simulate_lag else healthy_nodes

        for node in target_nodes:
            self.storage[node][key] = record
        return version_id


    def read(self, key: str, r: int = 2) -> Any | None:
        if not 1 <= r <= self.rf:
            raise ValueError
        replicas = self.ring.get_preference_list(key, rf=self.rf)

        healthy_replicas = [node for node in replicas if node not in self.down_nodes]

        if len(healthy_replicas) < r:
            raise QuorumError

        sampled_nodes = healthy_replicas[-r:]

        candidates = []
        for node in sampled_nodes:
            if key in self.storage[node]:
                candidates.append(self.storage[node][key])
        if not candidates:
            return None
        lastest_msg = max(candidates, key=lambda item:item['version_id'])
        return lastest_msg['value']

        