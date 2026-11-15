import threading
import time

class SnowflakeGenerator:
    EPOCH = 1704067200000
    NODE_ID_BITS = 10
    SEQUENCE_BITS = 12
    MAX_NODE_ID = (1 << NODE_ID_BITS) - 1 
    MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1
    NODE_ID_SHIFT = SEQUENCE_BITS
    TIMESTAMP_SHIFT = SEQUENCE_BITS + NODE_ID_BITS

    def __init__(self, node_id: int):
        if not isinstance(node_id, int):
            raise TypeError
        if not 0 <= node_id <= self.MAX_NODE_ID:
            raise ValueError
        
        self.node_id = node_id
        self.sequence = 0
        self.last_timestamp = -1
        self.lock = threading.Lock()
    @staticmethod
    def _current_ms():
        return int(time.time() * 1000)

    def next_id(self) -> int:
        with self.lock:
            current_ts = self._current_ms()
            if current_ts < self.last_timestamp:
               raise RuntimeError("Clock moved backwards!")
            if current_ts == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.MAX_SEQUENCE
                if self.sequence == 0:
                    while current_ts <= self.last_timestamp:
                        current_ts = self._current_ms()
            else:
                self.sequence = 0
            self.last_timestamp = current_ts
            elapsed_ms = current_ts - self.EPOCH
            elapsed_ms <<= self.TIMESTAMP_SHIFT
            node_id = self.node_id << self.NODE_ID_SHIFT
            return elapsed_ms | node_id | self.sequence
        
    @classmethod
    def decode(cls, snowflake_id: int) -> dict:
        sequence = snowflake_id & cls.MAX_SEQUENCE
        node_id = (snowflake_id >> cls.NODE_ID_SHIFT) & cls.MAX_NODE_ID
        Timestamp = (snowflake_id >> cls.TIMESTAMP_SHIFT) + cls.EPOCH

        return {
            'Sequence' : sequence,
            'Node_ID' : node_id,
            'Timestamp': Timestamp
        }