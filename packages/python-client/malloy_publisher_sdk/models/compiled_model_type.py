from enum import Enum


class CompiledModelType(str, Enum):
    NOTEBOOK = "notebook"
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
