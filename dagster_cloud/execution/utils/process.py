import os
import subprocess
import sys
from typing import List

from dagster.serdes.ipc import interrupt_ipc_subprocess_pid

from . import TaskStatus


def launch_process(args: List[str]) -> int:
    """
    Launch a process and return the PID
    """
    p = subprocess.Popen(args)
    pid = p.pid
    return pid


def check_on_process(pid: int) -> TaskStatus:
    # TODO: implement cross platform process check
    if sys.platform == "win32":
        return TaskStatus.NOT_IMPLEMENTED
    else:
        try:
            # Send a no-op. If the process is not running, it will respond with an error
            # https://stackoverflow.com/a/568285/14656695
            os.kill(pid, 0)
        except OSError:
            return TaskStatus.NOT_FOUND
        else:
            return TaskStatus.RUNNING


def kill_process(pid: int):
    interrupt_ipc_subprocess_pid(pid)
