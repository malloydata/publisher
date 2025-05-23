from enum import Enum


class ConnectionType(str, Enum):
    BIGQUERY = "bigquery"
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"
    TRINO = "trino"

    def __str__(self) -> str:
        return str(self.value)
