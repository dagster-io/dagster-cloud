import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import dagster._check as check
import grpc
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._grpc.__generated__ import api_pb2
from dagster._grpc.__generated__.api_pb2_grpc import (
    DagsterApiServicer,
    add_DagsterApiServicer_to_server,
)
from dagster._grpc.server import server_termination_target
from dagster._grpc.types import GetCurrentRunsResult
from dagster._grpc.utils import max_rx_bytes, max_send_bytes
from dagster._serdes import deserialize_as, serialize_dagster_namedtuple
from dagster._utils.error import serializable_error_info_from_exc_info
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from ..__generated__ import multi_pex_api_pb2
from ..__generated__.multi_pex_api_pb2_grpc import (
    MultiPexApiServicer,
    add_MultiPexApiServicer_to_server,
)
from ..types import (
    CreatePexServerArgs,
    CreatePexServerResponse,
    GetPexServersArgs,
    GetPexServersResponse,
    PexServerHandle,
    ShutdownPexServerArgs,
    ShutdownPexServerResponse,
)
from .manager import MultiPexManager


class MultiPexApiServer(MultiPexApiServicer):
    def __init__(
        self,
        pex_manager: MultiPexManager,
    ):
        self._pex_manager = pex_manager
        self.__cleanup_thread = threading.Thread(
            target=self._cleanup_thread, args=(), name="multi-pex-cleanup"
        )
        self.__cleanup_thread.daemon = True
        self.__cleanup_thread.start()

    def CreatePexServer(self, request, _context):
        create_pex_server_args = deserialize_as(request.create_pex_server_args, CreatePexServerArgs)
        try:
            self._pex_manager.create_pex_server(
                create_pex_server_args.server_handle,
                create_pex_server_args.code_deployment_metadata,
                create_pex_server_args.instance_ref,
            )
            response = CreatePexServerResponse()
        except:
            response = serializable_error_info_from_exc_info(sys.exc_info())

        return multi_pex_api_pb2.CreatePexServerReply(
            create_pex_server_response=serialize_dagster_namedtuple(response)
        )

    def GetPexServers(self, request, _context):
        get_pex_servers_args = deserialize_as(request.get_pex_servers_args, GetPexServersArgs)
        try:
            pex_server_handles = self._pex_manager.get_pex_servers(
                get_pex_servers_args.deployment_name,
                get_pex_servers_args.location_name,
            )
            response = GetPexServersResponse(server_handles=pex_server_handles)
        except:
            response = serializable_error_info_from_exc_info(sys.exc_info())

        return multi_pex_api_pb2.GetPexServersReply(
            get_pex_servers_response=serialize_dagster_namedtuple(response)
        )

    def ShutdownPexServer(self, request, _context):
        shutdown_pex_server_args = deserialize_as(
            request.shutdown_pex_server_args, ShutdownPexServerArgs
        )
        try:
            self._pex_manager.shutdown_pex_server(shutdown_pex_server_args.server_handle)
            response = ShutdownPexServerResponse()
        except:
            response = serializable_error_info_from_exc_info(sys.exc_info())
        return multi_pex_api_pb2.ShutdownPexServerReply(
            shutdown_pex_server_response=serialize_dagster_namedtuple(response)
        )

    def Ping(self, request, _context):
        echo = request.echo
        return multi_pex_api_pb2.PingReply(echo=echo)

    def _cleanup_thread(self):
        while True:
            time.sleep(5)
            self._pex_manager.cleanup_pending_shutdown_pex_servers()


class DagsterPexProxyApiServer(DagsterApiServicer):
    def __init__(self, pex_manager: MultiPexManager):
        self._pex_manager = pex_manager

    def _get_handle_from_metadata(self, context) -> PexServerHandle:
        metadict = dict(context.invocation_metadata())

        if "deployment" not in metadict:
            raise Exception("missing `deployment` in request metadata")

        if "location" not in metadict:
            raise Exception("missing `location` in request metadata")

        if "timestamp" not in metadict:
            raise Exception("missing `timestamp` in request metadata")

        return PexServerHandle(
            deployment_name=metadict["deployment"],
            location_name=metadict["location"],
            metadata_update_timestamp=int(metadict["timestamp"]),
        )

    def _query(self, api_name: str, request, context):
        return self._pex_manager.get_pex_grpc_client(  # pylint:disable=protected-access
            self._get_handle_from_metadata(context)
        )._get_response(api_name, request)

    def _streaming_query(self, api_name: str, request, context):
        return self._pex_manager.get_pex_grpc_client(  # pylint:disable=protected-access
            self._get_handle_from_metadata(context)
        )._get_streaming_response(api_name, request)

    def ExecutionPlanSnapshot(self, request, context):
        return self._query("ExecutionPlanSnapshot", request, context)

    def ListRepositories(self, request, context):
        return self._query("ListRepositories", request, context)

    def Ping(self, request, context):
        return self._query("Ping", request, context)

    def GetServerId(self, request, context):
        return self._query("GetServerId", request, context)

    def GetCurrentImage(self, request, context):
        return self._query("GetCurrentImage", request, context)

    def StreamingExternalRepository(self, request, context):
        return self._streaming_query("StreamingExternalRepository", request, context)

    def Heartbeat(self, request, context):
        return self._query("Heartbeat", request, context)

    def StreamingPing(self, request, context):
        return self._streaming_query("StreamingPing", request, context)

    def ExternalPartitionNames(self, request, context):
        return self._query("ExternalPartitionNames", request, context)

    def ExternalNotebookData(self, request, context):
        return self._query("ExternalNotebookData", request, context)

    def ExternalPartitionConfig(self, request, context):
        return self._query("ExternalPartitionConfig", request, context)

    def ExternalPartitionTags(self, request, context):
        return self._query("ExternalPartitionTags", request, context)

    def ExternalPartitionSetExecutionParams(self, request, context):
        return self._streaming_query("ExternalPartitionSetExecutionParams", request, context)

    def ExternalPipelineSubsetSnapshot(self, request, context):
        return self._query("ExternalPipelineSubsetSnapshot", request, context)

    def ExternalRepository(self, request, context):
        return self._query("ExternalRepository", request, context)

    def ExternalJob(self, request, context):
        return self._query("ExternalJob", request, context)

    def ExternalScheduleExecution(self, request, context):
        return self._streaming_query("ExternalScheduleExecution", request, context)

    def ExternalSensorExecution(self, request, context):
        return self._streaming_query("ExternalSensorExecution", request, context)

    def ShutdownServer(self, request, context):
        return self._query("ShutdownServer", request, context)

    def CancelExecution(self, request, context):
        return self._query("CancelExecution", request, context)

    def CanCancelExecution(self, request, context):
        return self._query("CanCancelExecution", request, context)

    def StartRun(self, request, context):
        return self._query("StartRun", request, context)

    def GetCurrentRuns(self, request, context):
        """
        Collect all run ids across all pex servers.
        """
        metadict = dict(context.invocation_metadata())

        if "deployment" in metadict:
            raise Exception(
                "GetCurrentRuns should not be called with grpc metadata. It applies to all pex"
                " servers"
            )

        if "location" in metadict:
            raise Exception(
                "GetCurrentRuns should not be called with grpc metadata. It applies to all pex"
                " servers"
            )

        if "timestamp" in metadict:
            raise Exception(
                "GetCurrentRuns should not be called with grpc metadata. It applies to all pex"
                " servers"
            )

        all_run_ids = []
        for handle_id, client in self._pex_manager.get_all_pex_grpc_clients_map().items():
            try:
                run_ids = deserialize_as(
                    client.get_current_runs(), GetCurrentRunsResult
                ).current_runs
                all_run_ids.extend(run_ids)
            except DagsterUserCodeUnreachableError:
                e = serializable_error_info_from_exc_info(sys.exc_info())

                # If the pex server is unreachable, it may just be in the process of shutting down.
                check.invariant(
                    not self._pex_manager.is_server_active(handle_id),
                    f"Active server hit error:\n{str(e)}",
                )

        return api_pb2.GetCurrentRunsReply(
            serialized_current_runs=serialize_dagster_namedtuple(
                GetCurrentRunsResult(current_runs=all_run_ids, serializable_error_info=None)
            )
        )


def run_multipex_server(
    port,
    socket,
    print_fn,
    host="localhost",
    max_workers=None,
    local_pex_files_dir: Optional[str] = "/tmp/pex-files",
):
    server = grpc.server(
        ThreadPoolExecutor(max_workers=max_workers),
        compression=grpc.Compression.Gzip,
        options=[
            ("grpc.max_send_message_length", max_send_bytes()),
            ("grpc.max_receive_message_length", max_rx_bytes()),
        ],
    )

    with MultiPexManager(local_pex_files_dir=local_pex_files_dir) as pex_manager:
        pex_api_servicer = MultiPexApiServer(
            pex_manager=pex_manager,
        )

        server_termination_event = threading.Event()

        dagster_api_servicer = DagsterPexProxyApiServer(pex_manager=pex_manager)
        health_servicer = health.HealthServicer()

        health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

        add_MultiPexApiServicer_to_server(pex_api_servicer, server)
        add_DagsterApiServicer_to_server(dagster_api_servicer, server)

        if port:
            server_address = host + ":" + str(port)
        else:
            server_address = "unix:" + os.path.abspath(socket)

        res = server.add_insecure_port(server_address)
        if (port and res != port) or (socket and res != 1):
            raise Exception(f"Could not bind to port {port}")

        server_desc = f"Pex server on port {port} in process {os.getpid()}"

        server.start()

        print_fn(f"Started {server_desc}")

        health_servicer.set("MultiPexApi", health_pb2.HealthCheckResponse.SERVING)

        server_termination_thread = threading.Thread(
            target=server_termination_target,
            args=[server_termination_event, server],
            name="grpc-server-termination",
        )
        server_termination_thread.daemon = True
        server_termination_thread.start()

        server.wait_for_termination()

        print_fn(f"Shutting down {server_desc}")
        server_termination_thread.join()

    print_fn("Server shut down.")
