from itertools import cycle


class DatabaseRouter:
    def __init__(self, primary_dsn: str, replica_dsns: list[str]):
        self.primary = primary_dsn
        self.replica_dsns = list(replica_dsns)
        self._replicas = cycle(self.replica_dsns) if self.replica_dsns else None

    def get_connection(self, operation: str, strong_consistency: bool = False) -> str:
        if operation not in ("read", "write"):
            raise ValueError("operation must be 'read' or 'write'")
        if operation == "write" or strong_consistency or self._replicas is None:
            return self.primary
        return next(self._replicas)

    def all_dsns(self) -> list[str]:
        return [self.primary, *self.replica_dsns]