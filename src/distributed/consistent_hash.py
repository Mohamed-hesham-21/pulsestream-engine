import hashlib
import bisect
class ConsistentHashRing:
    def __init__(self, nodes: list[str] | None = None, virtual_nodes: int = 100):
        if not virtual_nodes >= 1:
            raise ValueError
        self.virtual_nodes = virtual_nodes
        self.ring: dict[int, str] = {}
        self.sorted_keys: list[int] = []
        self.nodes: set[str] = set()
        if nodes:
            for node in nodes:
                self.add_node(node)
    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node):
        if node in self.nodes:
            return

        self.nodes.add(node)

        for i in range(0, self.virtual_nodes):
            virtual_node_key = f"{node}#vnode{i}"
            pos = self._hash(virtual_node_key)
            self.ring[pos] = node
            bisect.insort(self.sorted_keys, pos)

    def remove_node(self, node:str) -> None:
        if node not in self.nodes:
            raise KeyError(f"Node {node} not found")

        self.nodes.remove(node)

        for i in range(0, self.virtual_nodes):
            pos = self._hash(f"{node}#vnode{i}")
            self.ring.pop(pos)
            self.sorted_keys.remove(pos)

    def get_preference_list(self, key: str, rf: int = 3) -> list[str]:
        if not self.ring:
            return []
        if rf <= 0:
            raise ValueError
        
        key_hash = self._hash(key)
        start_idx = bisect.bisect_right(self.sorted_keys, key_hash)

        preference_list: list[str] = []

        for i in range(0, len(self.sorted_keys)):
            idx = (start_idx + i) % len(self.sorted_keys)
            physical_node = self.ring[self.sorted_keys[idx]]

            if physical_node not in preference_list:
                preference_list.append(physical_node)
            if len(preference_list) == min(rf, len(self.nodes)):
                break
        return preference_list

    def get_node(self, key: str) -> str | None:
        prefs =  self.get_preference_list(key, rf = 1)
        return prefs[0] if prefs else None