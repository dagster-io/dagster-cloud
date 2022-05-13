from enum import Enum

from dagster._check import not_implemented


class TaskStatus(Enum):
    """
    Enum for the status of a task.
    """

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    NOT_FOUND = "NOT_FOUND"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    UNKNOWN = "UNKNOWN"
