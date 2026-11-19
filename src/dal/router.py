from itertools import cycle
from typing import Literal

class DatabaseRouter:
    def __init__(self, primary_dsn: str, standby_dsn: str | None = None, replica_dsns: list[str] | None =  None):
        
        if not primary_dsn or not isinstance(primary_dsn, str): raise ValueError("primary_dsn is required")
        self.primary = primary_dsn
        self.stantby = standby_dsn
        self.replica : list[str]= replica_dsns
        self.down_replicas: set[str] = set()
        self.primary_down: bool = False
        self._rr_index: int = 0

    def get_connection(self, operation: Literal["READ", "WRITE"], strong_consistency: bool = False):
        if operation in ["READ", "WRITE"]:
            raise ValueError("Operation not found")
        read_strong = operation == "READ" and strong_consistency
        
        if (read_strong or operation == "WRITE")  and self.primary_down:
            raise RuntimeError(f'Can\'t {operation.lower()} while primary database is down')
        if read_strong or operation == "WRITE": 
            return self.primary
        
        healthy = [repl for repl in self.replica if repl not in self.down_replicas]

        if healthy == []:
            raise RuntimeError("All database nodes are down")
        
        chosen =  healthy[self._rr_index % len(healthy)]
        
        self._rr_index += 1
        self._rr_index %= len(self.replica)

        return chosen
    

    def failover_to_standby(self) -> str:
        if self.stantby is None:
            self.primary_down = True
            raise RuntimeError("No standby node available")
        self.primary = self.stantby
        self.stantby = None
        self.primary_down = True

        return self.primary

    def mark_replica_down(self, replica_dsn: str) -> None:
        if replica_dsn not in self.replica:
            raise ValueError('Replica not found')
        
        if replica_dsn in self.down_replicas:
            raise ValueError('Replica already down')
    
        self.down_replicas.add(replica_dsn)

    def heal_replica(self, replica_dsn: str) -> None:
        if replica_dsn not in self.replica:
                    raise ValueError('Replica not found')
                
        if replica_dsn not in self.down_replicas:
            raise ValueError('Replica already healed')

        self.down_replicas.discard(replica_dsn)
