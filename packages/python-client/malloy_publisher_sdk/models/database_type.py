from enum import Enum


class DatabaseType(str, Enum):
    EMBEDDED = "embedded"
    MATERIALIZED = "materialized"

    def __str__(self) -> str:
        return str(self.value)
