import os
import threading
from contextlib import AbstractContextManager
from typing import Dict, List, NamedTuple

from dagster import _check as check
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._grpc.client import DagsterGrpcClient, client_heartbeat_thread
from dagster._grpc.server import GrpcServerProcess
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata, PexMetadata

from .grpc.types import PexServerHandle


class PexProcessEntry(
    NamedTuple(
        "_PexProcessEntry",
        [
            ("pex_server_handle", PexServerHandle),
            ("grpc_server_process", GrpcServerProcess),
            ("grpc_client", DagsterGrpcClient),
            ("heartbeat_shutdown_event", threading.Event),
            ("heartbeat_thread", threading.Thread),
        ],
    )
):
    def __new__(
        cls,
        pex_server_handle: PexServerHandle,
        grpc_server_process: GrpcServerProcess,
        grpc_client: DagsterGrpcClient,
        heartbeat_shutdown_event: threading.Event,
        heartbeat_thread: threading.Thread,
    ):
        return super(PexProcessEntry, cls).__new__(
            cls,
            check.inst_param(pex_server_handle, "pex_server_handle", PexServerHandle),
            check.inst_param(grpc_server_process, "grpc_server_process", GrpcServerProcess),
            check.inst_param(grpc_client, "grpc_client", DagsterGrpcClient),
            check.inst_param(heartbeat_shutdown_event, "heartbeat_shutdown_event", threading.Event),
            check.inst_param(heartbeat_thread, "heartbeat_thread", threading.Thread),
        )


class MultiPexManager(AbstractContextManager):
    def __init__(
        self,
    ):
        # Keyed by hash of PexServerHandle
        self._pex_servers: Dict[str, PexProcessEntry] = {}
        self._pex_servers_lock = threading.Lock()
        self._heartbeat_ttl = 60

    def download_pex_from_s3(self, _pex_metadata: PexMetadata):
        # TODO Implement for real
        return "./source.pex"

    def get_pex_grpc_client(self, server_handle: PexServerHandle):
        handle_id = server_handle.get_id()
        with self._pex_servers_lock:
            if not handle_id in self._pex_servers:
                raise Exception("No server created with the given handle")

            return self._pex_servers[handle_id].grpc_client

    def get_pex_servers(self, deployment_name, location_name: str) -> List[PexServerHandle]:
        with self._pex_servers_lock:
            return [
                server.pex_server_handle
                for server in self._pex_servers.values()
                if server.pex_server_handle.deployment_name == deployment_name
                and server.pex_server_handle.location_name == location_name
            ]

    def create_pex_server(
        self,
        server_handle: PexServerHandle,
        code_deployment_metadata: CodeDeploymentMetadata,
    ):
        print(
            f"Creating new pex server for {server_handle.deployment_name}:{server_handle.location_name}"
        )

        executable_path = self.download_pex_from_s3(
            check.not_none(code_deployment_metadata.pex_metadata)
        )

        metadata = code_deployment_metadata

        loadable_target_origin = LoadableTargetOrigin(
            executable_path=executable_path,
            python_file=metadata.python_file,
            package_name=metadata.package_name,
            module_name=metadata.module_name,
            working_directory=metadata.working_directory,
            attribute=metadata.attribute,
        )

        server_process = GrpcServerProcess(
            loadable_target_origin=loadable_target_origin,
            heartbeat=True,
            heartbeat_timeout=self._heartbeat_ttl,
            startup_timeout=0,  # don't wait for startup, agent will poll for status
            log_level="INFO",
            # Temporary for testing
            env={**os.environ.copy(), "PEX_PATH": os.path.join(os.getcwd(), "deps.pex")},
        )

        client = DagsterGrpcClient(
            port=server_process.port,
            socket=server_process.socket,
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

        # open question - how do we know when to spin down old pexes / how do you reload a pex
        # in a zero-downtime way (very similar questions to the process agent really)
        with self._pex_servers_lock:
            self._pex_servers[server_handle.get_id()] = PexProcessEntry(
                pex_server_handle=server_handle,
                grpc_server_process=server_process,
                grpc_client=client,
                heartbeat_shutdown_event=heartbeat_shutdown_event,
                heartbeat_thread=heartbeat_thread,
            )

    def shutdown_pex_server(self, server_handle: PexServerHandle):
        handle_id = server_handle.get_id()
        pex_server = None
        with self._pex_servers_lock:
            pex_server = self._pex_servers.get(handle_id)
            if pex_server:
                del self._pex_servers[handle_id]

        if pex_server:
            pex_server.heartbeat_shutdown_event.set()
            pex_server.heartbeat_thread.join()
            try:
                pex_server.grpc_client.shutdown_server()
            except DagsterUserCodeUnreachableError:
                # Server already shutdown
                pass
            pex_server.grpc_server_process.wait()

    def __exit__(self, exception_type, exception_value, traceback):
        for _handle, pex_server in self._pex_servers.items():
            pex_server.heartbeat_shutdown_event.set()
            pex_server.heartbeat_thread.join()

        for _handle, pex_server in self._pex_servers.items():
            try:
                pex_server.grpc_client.shutdown_server()
            except DagsterUserCodeUnreachableError:
                # Server already shutdown
                pass
            pex_server.grpc_server_process.wait()
