import subprocess
from abc import abstractmethod
from enum import Enum
from queue import Queue
from threading import Thread
from typing import Dict, Iterable, NamedTuple, Tuple

from dagster_cloud.api.dagster_cloud_api import DagsterCloudSandboxConnectionInfo
from dagster_cloud.cli.sandbox.utils import get_current_display_timestamp

from .. import ui


class SyncedDirectory(NamedTuple):
    identifier: str
    location_name: str
    connection_info: DagsterCloudSandboxConnectionInfo
    from_directory: str
    to_directory: str


class SyncState(Enum):
    SYNCED = "SYNCED"
    SYNCING = "SYNCING"
    UNAVAILABLE = "ERRORED"


class SyncMethod:
    @abstractmethod
    def preflight(self, verbosity_level: int) -> None:
        """
        Validates that this sync method is available.
        """

    @abstractmethod
    def create_directory_sync(
        self,
        information: SyncedDirectory,
    ) -> None:
        """
        Set up a directory to be synced.
        """

    @abstractmethod
    def sync_loop(self) -> Iterable[Tuple[str, SyncState]]:
        """
        Synchronize all set up directories. This method should run until interrupted,
        yielding an (identifier, SyncState) tuple when a state change is detected.

        For active sync methods managed by the CLI, this loop may actually conduct the
        synchronization process. For methods managed externally, such as Mutagen, this
        loop is only responsible for monitoring the state of the synchronization process.
        """

    @abstractmethod
    def cleanup_directory_sync(self, identifier: str) -> None:
        """
        Stop and clean up any information associated with a synced directory.
        """


class MutagenSyncMethod(SyncMethod):
    """
    Synchronization method which relies on the Mutagen CLI to transport
    files between the source and destination over SSH.
    """

    def __init__(self):
        self.targets: Dict[str, SyncedDirectory] = {}
        self.monitor_threads: Dict[str, Thread] = {}
        self.verbosity_level: int = 0

    def preflight(self, verbosity_level: int) -> None:
        try:
            subprocess.check_output(["mutagen", "version"])
        except subprocess.CalledProcessError:
            raise ui.error(
                f"{ui.as_code('mutagen')} executable not found. You must install mutagen "
                f"in order to use code syncing functionality.\n\nRun {ui.as_code('brew install mutagen-io/mutagen/mutagen')} "
                f"or see {ui.as_code('https://mutagen.io/documentation/introduction/installation')}."
            )
        self.verbosity_level = verbosity_level

    def create_directory_sync(
        self,
        information: SyncedDirectory,
    ) -> None:
        subprocess.check_output(
            [
                "mutagen",
                "sync",
                "create",
                information.from_directory,
                (
                    f"{information.connection_info.username}@{information.connection_info.hostname}"
                    f":{information.connection_info.port}:{information.to_directory}"
                ),
                f"--name={information.identifier}",
                "--sync-mode",
                "one-way-replica",
                "--watch-mode-beta=no-watch",
                # Start sessions paused so we get all lifecycle events
                # unpause during sync loop
                "--paused",
            ]
        )
        self.targets[information.identifier] = information

    def sync_loop(self) -> Iterable[Tuple[str, SyncState]]:
        # https://stackoverflow.com/a/4896288
        def pass_console_output(identifier, out, queue):
            for stdout_line in iter(out.readline, ""):
                queue.put((identifier, stdout_line))
            out.close()

        console_out_queue: Queue = Queue()
        for identifier in self.targets:
            popen = subprocess.Popen(
                ["mutagen", "sync", "monitor", identifier],
                stdout=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
            )
            t = Thread(
                target=pass_console_output, args=(identifier, popen.stdout, console_out_queue)
            )
            t.daemon = True
            t.start()
            self.monitor_threads[identifier] = t

            subprocess.check_output(["mutagen", "sync", "resume", identifier])
        while True:
            identifier, stdout_line = console_out_queue.get()

            if self.verbosity_level >= 2:
                print(f"[{get_current_display_timestamp()}] [mutagen] {stdout_line.strip()}")

            # List of states:
            # https://github.com/mutagen-io/mutagen/blob/master/pkg/synchronization/state.proto
            if (
                "Applying" in stdout_line
                or "Staging" in stdout_line
                or "Reconciling" in stdout_line
            ):
                yield (identifier, SyncState.SYNCING)

            elif "Saving archive" in stdout_line:
                yield (identifier, SyncState.SYNCED)

            elif "Errored" in stdout_line:
                yield (identifier, SyncState.UNAVAILABLE)

    def cleanup_directory_sync(self, identifier: str) -> None:
        subprocess.check_output(["mutagen", "sync", "terminate", identifier])
        del self.targets[identifier]
        self.monitor_threads[identifier].join()
        del self.monitor_threads[identifier]
