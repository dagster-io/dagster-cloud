import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc
from dagster._grpc.__generated__.api_pb2_grpc import (
    DagsterApiServicer,
    add_DagsterApiServicer_to_server,
)
from dagster._grpc.server import server_termination_target
from dagster._grpc.utils import max_rx_bytes, max_send_bytes
from dagster._serdes import deserialize_as, serialize_dagster_namedtuple
from dagster._utils.error import serializable_error_info_from_exc_info
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from ..manager import MultiPexManager
from .__generated__ import multi_pex_api_pb2
from .__generated__.multi_pex_api_pb2_grpc import (
    MultiPexApiServicer,
    add_MultiPexApiServicer_to_server,
)
from .types import (
    CreatePexServerArgs,
    CreatePexServerResponse,
    GetPexServersArgs,
    GetPexServersResponse,
    PexServerHandle,
    ShutdownPexServerArgs,
    ShutdownPexServerResponse,
)


class MultiPexApiServer(MultiPexApiServicer):
    def __init__(
        self,
        pex_manager: MultiPexManager,
    ):
        self._pex_manager = pex_manager

    def CreatePexServer(self, request, _context):
        create_pex_server_args = deserialize_as(request.create_pex_server_args, CreatePexServerArgs)
        try:
            self._pex_manager.create_pex_server(
                create_pex_server_args.server_handle,
                create_pex_server_args.code_deployment_metadata,
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
        return self._query("ExternalPartitionSetExecutionParams", request, context)

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


def run_multipex_server(port, print_fn, host="localhost", max_workers=None):
    server = grpc.server(
        ThreadPoolExecutor(max_workers=max_workers),
        compression=grpc.Compression.Gzip,
        options=[
            ("grpc.max_send_message_length", max_send_bytes()),
            ("grpc.max_receive_message_length", max_rx_bytes()),
        ],
    )

    with MultiPexManager() as pex_manager:
        pex_api_servicer = MultiPexApiServer(
            pex_manager=pex_manager,
        )

        server_termination_event = threading.Event()

        dagster_api_servicer = DagsterPexProxyApiServer(pex_manager=pex_manager)
        health_servicer = health.HealthServicer()

        health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

        add_MultiPexApiServicer_to_server(pex_api_servicer, server)
        add_DagsterApiServicer_to_server(dagster_api_servicer, server)

        server_address = host + ":" + str(port)
        res = server.add_insecure_port(server_address)
        if res != port:
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
        pex_api_servicer.cleanup()
        dagster_api_servicer.cleanup()

    print_fn("Server shut down.")


def wait_for_grpc_server(client, timeout=180):
    start_time = time.time()

    while True:
        try:
            client.ping("")
            return
        except Exception:
            last_error = serializable_error_info_from_exc_info(sys.exc_info())
            print(str(last_error))

        if time.time() - start_time > timeout:
            raise Exception(
                f"Timed out after waiting {timeout}s for server. "
                f"Most recent connection error: {str(last_error)}"
            )

        time.sleep(1)
