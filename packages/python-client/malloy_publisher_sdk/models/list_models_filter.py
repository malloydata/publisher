from enum import Enum


class ListModelsFilter(str, Enum):
    ALL = "all"
    NOTEBOOK = "notebook"
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
