# Send keys off to be processed, we will have each key == a window in time
from enum import Enum
from typing import Any


class KeyedValue(object) :

    def __init__(self, key: Any, value: Any):
        self.key = key
        self.value = value


class StageWorkerStatus(Enum):
    STOPPED = 1
    PROCESSING = 2

