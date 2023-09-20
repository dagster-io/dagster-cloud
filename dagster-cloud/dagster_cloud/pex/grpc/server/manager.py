import logging
import os
import subprocess
import sys
import threading
import time
from contextlib import AbstractContextManager
from typing import Dict, List, Optional, Set, Tuple, Union, cast

import dagster._seven as seven
from dagster import _check as check
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.instance.ref import InstanceRef
from dagster._grpc.client import DagsterGrpcClient, client_heartbeat_thread
from dagster._serdes.ipc import open_ipc_subprocess
from dagster._utils import find_free_port, safe_tempfile_path_unmanaged
from dagster._utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata
from pydantic import BaseModel, Extra

from ..types import PexServerHandle
from .registry import PexS3Registry


class PexProcessEntry(BaseModel, frozen=True, extra=Extra.forbid, arbitrary_types_allowed=True):
    pex_server_handle: PexServerHandle
    grpc_server_process: subprocess.Popen
    grpc_client: DagsterGrpcClient
    heartbeat_shutdown_event: threading.Event
    heartbeat_thread: threading.Thread


class PexErrorEntry(BaseModel, frozen=True, extra=Extra.forbid, arbitrary_types_allowed=True):
    pex_server_handle: PexServerHandle
    error: SerializableErrorInfo


class MultiPexManager(AbstractContextManager):
    def __init__(
        self,
        local_pex_files_dir: Optional[str] = None,
        watchdog_run_interval: Optional[int] = 30,
    ):
        # Keyed by hash of PexServerHandle
        self._pex_servers: Dict[str, Union[PexProcessEntry, PexErrorEntry]] = {}
        self._pending_startup_pex_servers: Set[str] = set()
        self._pending_shutdown_pex_servers: Set[str] = set()
        self._pex_servers_lock = threading.RLock()
        self._heartbeat_ttl = 60
        self._registry = PexS3Registry(local_pex_files_dir)

        if watchdog_run_interval and watchdog_run_interval > 0:
            self._watchdog_thread = threading.Thread(
                name="watchdog",
                target=self.run_watchdog_thread,
                args=(watchdog_run_interval,),
                daemon=True,
            )
            self._watchdog_thread.start()
            logging.info(
                "Created a watchdog thread %s for MultiPexManager with watchdog_run_interval=%s",
                self._watchdog_thread.name,
                watchdog_run_interval,
            )
        else:
            logging.info(
                "No watchdog thread started for MultiPexManager (watchdog_run_interval=%s)",
                watchdog_run_interval,
            )

    def run_watchdog_thread(self, interval: int) -> None:
        # Regularly check for unexpected terminations of all active code servers
        # and update status to error in _pex_servers
        while True:
            time.sleep(interval)
            active_servers = self.get_active_pex_servers()
            dead_server_returncodes = []
            for server in active_servers:
                server_id = server.pex_server_handle.get_id()
                returncode = server.grpc_server_process.poll()
                if returncode is not None:
                    dead_server_returncodes.append((server, returncode))
                    logging.error(
                        "watchdog: pex subprocesss %s unexpectedly exited with returncode %s -"
                        " changing state to error",
                        server_id,
                        returncode,
                    )
            self._mark_servers_unexpected_termination(dead_server_returncodes)
            logging.info(
                "watchdog: inspected %s active servers %s, of which %s were found unexpectedly"
                " terminated",
                len(active_servers),
                [server.pex_server_handle.get_id() for server in active_servers],
                len(dead_server_returncodes),
            )

    def _mark_servers_unexpected_termination(
        self, dead_server_returncodes: List[Tuple[PexProcessEntry, int]]
    ) -> None:
        with self._pex_servers_lock:
            for server, returncode in dead_server_returncodes:
                handle_id = server.pex_server_handle.get_id()
                if not self.is_server_active(handle_id):
                    # something already marked this server non active, do not overwrite the sate
                    continue
                self._pex_servers[handle_id] = PexErrorEntry(
                    pex_server_handle=server.pex_server_handle,
                    error=SerializableErrorInfo(
                        message=f"code server subprocess unexpectedly exited with {returncode}:",
                        cls_name=None,
                        stack=[],
                    ),
                )

    def get_pex_grpc_client_or_error(
        self, server_handle: PexServerHandle
    ) -> Union[DagsterGrpcClient, SerializableErrorInfo]:
        handle_id = server_handle.get_id()
        with self._pex_servers_lock:
            if handle_id not in self._pex_servers:
                if handle_id in self._pending_startup_pex_servers:
                    raise Exception("This server is still starting up")
                else:
                    raise Exception("No server created with the given handle")

            pex_server_or_error = self._pex_servers[handle_id]
            if isinstance(pex_server_or_error, PexErrorEntry):
                return pex_server_or_error.error

            return cast(PexProcessEntry, self._pex_servers[handle_id]).grpc_client

    def get_active_pex_servers(self) -> List[PexProcessEntry]:
        with self._pex_servers_lock:
            return [
                server
                for server_id, server in self._pex_servers.items()
                if self.is_server_active(server_id) and isinstance(server, PexProcessEntry)
            ]

    def get_active_pex_server_handles(
        self, deployment_name, location_name: str
    ) -> List[PexServerHandle]:
        return [
            server.pex_server_handle
            for server in self.get_active_pex_servers()
            if server.pex_server_handle.deployment_name == deployment_name
            and server.pex_server_handle.location_name == location_name
        ]

    def get_all_pex_grpc_clients_map(self) -> Dict[str, DagsterGrpcClient]:
        with self._pex_servers_lock:
            return {
                server.pex_server_handle.get_id(): server.grpc_client
                for server in self._pex_servers.values()
                if isinstance(server, PexProcessEntry)
            }

    def is_server_active(self, server_handle_id: str) -> bool:
        """Server is present and not pending shutdown."""
        with self._pex_servers_lock:
            return (
                server_handle_id in self._pex_servers
                and server_handle_id not in self._pending_shutdown_pex_servers
                and isinstance(self._pex_servers[server_handle_id], PexProcessEntry)
            )

    def create_pex_server(
        self,
        server_handle: PexServerHandle,
        code_deployment_metadata: CodeDeploymentMetadata,
        instance_ref: Optional[InstanceRef],
    ):
        # install pex files and launch them - do it asynchronously to not block this call
        def _create_pex_server() -> None:
            try:
                pex_executable = self._registry.get_pex_executable(
                    check.not_none(code_deployment_metadata.pex_metadata)
                )
                logging.info(
                    "Installed pex executable %s at %s",
                    code_deployment_metadata.pex_metadata,
                    pex_executable.source_path,
                )

                metadata = code_deployment_metadata
                logging.info("Launching subprocess %s", pex_executable.source_path)
                subprocess_args = [
                    pex_executable.source_path,
                    "-m",
                    "dagster",
                    "api",
                    "grpc",
                    "--heartbeat",
                    "--heartbeat-timeout",
                    str(self._heartbeat_ttl),
                ]

                if seven.IS_WINDOWS:
                    port = find_free_port()
                    socket = None
                else:
                    port = None
                    socket = safe_tempfile_path_unmanaged()

                additional_env = metadata.get_grpc_server_env(
                    port=port,
                    location_name=server_handle.location_name,
                    instance_ref=instance_ref,
                    socket=socket,
                )

                server_process = open_ipc_subprocess(
                    subprocess_args,
                    env={
                        **os.environ.copy(),
                        **pex_executable.environ,
                        **additional_env,
                    },
                    cwd=pex_executable.working_directory,
                )

                client = DagsterGrpcClient(
                    port=port,
                    socket=socket,
                    host="localhost",
                    use_ssl=False,
                )

                heartbeat_shutdown_event = threading.Event()
                heartbeat_thread = threading.Thread(
                    target=client_heartbeat_thread,
                    args=(client, heartbeat_shutdown_event),
                )
                heartbeat_thread.daemon = True
                heartbeat_thread.start()
                logging.info(
                    "Created a heartbeat thread %s for %s",
                    heartbeat_thread.name,
                    server_handle.get_id(),
                )

            except Exception:
                with self._pex_servers_lock:
                    self._pex_servers[server_handle.get_id()] = PexErrorEntry(
                        pex_server_handle=server_handle,
                        error=serializable_error_info_from_exc_info(sys.exc_info()),
                    )
                    self._pending_startup_pex_servers.remove(server_handle.get_id())
                logging.exception(
                    "Creating new pex server for %s:%s failed",
                    server_handle.deployment_name,
                    server_handle.location_name,
                )
                return

            with self._pex_servers_lock:
                self._pex_servers[server_handle.get_id()] = PexProcessEntry(
                    pex_server_handle=server_handle,
                    grpc_server_process=server_process,
                    grpc_client=client,
                    heartbeat_shutdown_event=heartbeat_shutdown_event,
                    heartbeat_thread=heartbeat_thread,
                )
                self._pending_startup_pex_servers.remove(server_handle.get_id())

        with self._pex_servers_lock:
            handle_id = server_handle.get_id()
            if handle_id in self._pending_startup_pex_servers:
                logging.info(
                    "Ignoring request to create pex server for %s - an identical server is"
                    " already pending start",
                    handle_id,
                )
                return
            self._pending_startup_pex_servers.add(handle_id)
            if handle_id in self._pex_servers:
                # clear any previous error state since we're attempting to start server again
                logging.info(
                    "Clearing previous state for %s: %s", handle_id, self._pex_servers[handle_id]
                )
                del self._pex_servers[handle_id]

        logging.info(
            "Creating new pex server for %s:%s",
            server_handle.deployment_name,
            server_handle.location_name,
        )
        threading.Thread(target=_create_pex_server).start()

    def shutdown_pex_server(self, server_handle: PexServerHandle):
        handle_id = server_handle.get_id()
        with self._pex_servers_lock:
            if handle_id in self._pex_servers or handle_id in self._pending_startup_pex_servers:
                logging.info("Server %s marked for shutdown", handle_id)
                self._pending_shutdown_pex_servers.add(handle_id)

    def cleanup_pending_shutdown_pex_servers(self) -> None:
        with self._pex_servers_lock:
            # clean up for any processes that have exited
            to_remove = set()
            for handle_id in self._pending_shutdown_pex_servers:
                if handle_id not in self._pex_servers:
                    continue
                server = self._pex_servers[handle_id]
                if (
                    isinstance(server, PexProcessEntry)
                    and server.grpc_server_process.poll() is not None
                ):
                    to_remove.add(handle_id)

            for handle_id in to_remove:
                logging.info("Server %s completely shutdown, cleaning up", handle_id)
                self._pending_shutdown_pex_servers.remove(handle_id)
                del self._pex_servers[handle_id]

            # request shutdown for processes that we have not tried to shutdown yet
            for handle_id in self._pending_shutdown_pex_servers:
                pex_server = self._pex_servers.get(handle_id)
                if not pex_server:
                    # still in _pending_startup_pex_servers
                    logging.info("Server %s not up yet, will request shutdown later", handle_id)
                    continue

                if isinstance(pex_server, PexErrorEntry):
                    logging.info("Server %s was in an error state, no shutdown needed", handle_id)
                    continue

                if pex_server.heartbeat_shutdown_event.is_set():
                    # already requested shutdown
                    logging.info("Already requested shutdown for server %s", handle_id)
                    continue

                logging.info("Requesting shutdown for server %s", handle_id)
                pex_server.heartbeat_shutdown_event.set()
                pex_server.heartbeat_thread.join()
                try:
                    pex_server.grpc_client.shutdown_server()
                except DagsterUserCodeUnreachableError:
                    # Server already shutdown
                    pass

    def __exit__(self, exception_type, exception_value, traceback):
        for pex_server in self._pex_servers.values():
            if isinstance(pex_server, PexProcessEntry):
                pex_server.heartbeat_shutdown_event.set()
                pex_server.heartbeat_thread.join()

        for pex_server in self._pex_servers.values():
            if isinstance(pex_server, PexProcessEntry):
                try:
                    pex_server.grpc_client.shutdown_server()
                except DagsterUserCodeUnreachableError:
                    # Server already shutdown
                    pass
                if pex_server.grpc_server_process.poll() is None:
                    pex_server.grpc_server_process.communicate(timeout=30)
