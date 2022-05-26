import concurrent.futures
import logging
import os
import sys
import tempfile
import time
import zlib
from collections import deque
from contextlib import ExitStack
from typing import Dict, Iterator, NamedTuple, Optional, Union, cast

import dagster._check as check
import pendulum
from dagster import DefaultRunLauncher
from dagster.core.errors import DagsterUserCodeUnreachableError
from dagster.core.host_representation import RepositoryLocationOrigin
from dagster.core.launcher.base import LaunchRunContext
from dagster.grpc.client import DagsterGrpcClient
from dagster.serdes import (
    deserialize_as,
    deserialize_json_to_dagster_namedtuple,
    serialize_dagster_namedtuple,
)
from dagster.utils import backoff, merge_dicts
from dagster.utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster.utils.interrupts import raise_interrupts_as
from dagster_cloud.api.dagster_cloud_api import (
    AgentHeartbeat,
    DagsterCloudApi,
    DagsterCloudApiErrorResponse,
    DagsterCloudApiGrpcResponse,
    DagsterCloudApiRequest,
    DagsterCloudApiResponse,
    DagsterCloudApiSuccess,
    DagsterCloudApiThreadTelemetry,
    DagsterCloudApiUnknownCommandResponse,
    DagsterCloudSandboxProxyInfo,
    DagsterCloudUploadApiResponse,
    TimestampedError,
)
from dagster_cloud.instance import DagsterCloudAgentInstance
from dagster_cloud.storage.errors import GraphQLStorageError
from dagster_cloud.workspace.origin import CodeDeploymentMetadata
from dagster_cloud.workspace.user_code_launcher import (
    DagsterCloudUserCodeLauncher,
    UserCodeLauncherEntry,
)

from ..errors import raise_http_error
from ..version import __version__
from .queries import (
    ADD_AGENT_HEARTBEAT_MUTATION,
    GET_USER_CLOUD_REQUESTS_QUERY,
    WORKSPACE_ENTRIES_QUERY,
)

CHECK_WORKSPACE_INTERVAL_SECONDS = 5

# Interval at which Agent heartbeats are sent to the host cloud
AGENT_HEARTBEAT_INTERVAL_SECONDS = 30

AGENT_HEARTBEAT_ERROR_LIMIT = 25  # Send at most 25 errors


class DagsterCloudApiFutureContext(
    NamedTuple(
        "_DagsterCloudApiFutureContext",
        [
            ("future", concurrent.futures.Future),
            ("timeout", float),
        ],
    )
):
    DEFAULT_TIMEOUT_SECONDS = 75

    def __new__(
        cls,
        future: concurrent.futures.Future,
        timeout: Optional[float] = None,
    ):
        return super(cls, DagsterCloudApiFutureContext).__new__(
            cls,
            check.inst_param(future, "future", concurrent.futures.Future),
            check.opt_float_param(
                timeout,
                "timeout",
                default=pendulum.now("UTC").add(seconds=cls.DEFAULT_TIMEOUT_SECONDS).timestamp(),
            ),
        )

    def timed_out(self):
        return pendulum.now("UTC").timestamp() > self.timeout


class DagsterCloudAgent:
    MAX_THREADS_PER_CORE = 10

    def __init__(self):
        self._logger = logging.getLogger("dagster_cloud")

        self._logger.info("Starting Dagster Cloud agent...")

        self._exit_stack = ExitStack()
        self._iteration = 0

        max_workers = os.cpu_count() * self.MAX_THREADS_PER_CORE
        self._executor = self._exit_stack.enter_context(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="dagster_cloud_agent_worker",
            )
        )

        self._request_ids_to_future_context: Dict[str, DagsterCloudApiFutureContext] = {}

        self._last_heartbeat_time = None

        self._last_workspace_check_time = None

        self._errors = deque(
            maxlen=AGENT_HEARTBEAT_ERROR_LIMIT
        )  # (SerializableErrorInfo, timestamp) tuples

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception_value, _traceback):
        self._exit_stack.close()

    def initialize_workspace(self, instance, user_code_launcher):
        self._check_update_workspace(instance, user_code_launcher)

        self._logger.info("Loading Dagster repositories...")
        while not user_code_launcher.is_workspace_ready:
            # Check for any received interrupts
            with raise_interrupts_as(KeyboardInterrupt):
                time.sleep(5)

    def run_loop(self, instance, user_code_launcher, agent_uuid):
        heartbeat_interval_seconds = AGENT_HEARTBEAT_INTERVAL_SECONDS

        self.initialize_workspace(instance, user_code_launcher)

        self._logger.info("Started polling for requests from {}".format(instance.dagster_cloud_url))

        while True:
            try:
                for error in self.run_iteration(instance, user_code_launcher):
                    if error:
                        self._logger.error(str(error))
                        self._errors.appendleft((error, pendulum.now("UTC")))
            except Exception:
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error("Caught error:\n{}".format(error_info))
                self._errors.appendleft((error_info, pendulum.now("UTC")))

            # Check for any received interrupts
            with raise_interrupts_as(KeyboardInterrupt):
                pass

            try:
                self._check_add_heartbeat(instance, agent_uuid, heartbeat_interval_seconds)
            except Exception:
                self._logger.error(
                    "Failed to add heartbeat: \n{}".format(
                        serializable_error_info_from_exc_info(sys.exc_info())
                    )
                )

            # Check for any received interrupts
            with raise_interrupts_as(KeyboardInterrupt):
                pass

            try:
                self._check_update_workspace(instance, user_code_launcher)

            except Exception:
                self._logger.error(
                    "Failed to check for workspace updates: \n{}".format(
                        serializable_error_info_from_exc_info(sys.exc_info())
                    )
                )

            # Check for any received interrupts
            with raise_interrupts_as(KeyboardInterrupt):
                time.sleep(0.5)

    def _check_update_workspace(self, instance, user_code_launcher):
        curr_time = pendulum.now("UTC")

        if (
            self._last_workspace_check_time
            and (curr_time - self._last_workspace_check_time).total_seconds()
            < CHECK_WORKSPACE_INTERVAL_SECONDS
        ):
            return

        self._last_workspace_check_time = curr_time

        self._query_for_workspace_updates(instance, user_code_launcher, upload_results=False)

    def _check_add_heartbeat(self, instance, agent_uuid, heartbeat_interval_seconds):
        curr_time = pendulum.now("UTC")

        if (
            self._last_heartbeat_time
            and (curr_time - self._last_heartbeat_time).total_seconds() < heartbeat_interval_seconds
        ):
            return

        errors = [
            TimestampedError(timestamp.float_timestamp, error)
            for (error, timestamp) in self._errors
        ]

        run_worker_statuses = instance.user_code_launcher.get_cloud_run_worker_statuses()

        self._last_heartbeat_time = curr_time
        heartbeat = AgentHeartbeat(
            timestamp=curr_time.float_timestamp,
            agent_id=agent_uuid,
            agent_label=instance.dagster_cloud_api_agent_label,
            agent_type=type(instance.user_code_launcher).__name__
            if instance.user_code_launcher
            else None,
            errors=errors,
            metadata={"version": __version__},
            run_worker_statuses=run_worker_statuses,
        )

        res = instance.graphql_client.execute(
            ADD_AGENT_HEARTBEAT_MUTATION,
            variable_values={"serializedAgentHeartbeat": serialize_dagster_namedtuple(heartbeat)},
        )
        if "errors" in res:
            raise GraphQLStorageError(res)

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        return self._executor

    @property
    def request_ids_to_future_context(self) -> Dict[str, DagsterCloudApiFutureContext]:
        return self._request_ids_to_future_context

    def _query_for_workspace_updates(
        self,
        instance: DagsterCloudAgentInstance,
        user_code_launcher: DagsterCloudUserCodeLauncher,
        upload_results: bool,
    ):
        # Get list of workspace entries from DB
        result = instance.graphql_client.execute(WORKSPACE_ENTRIES_QUERY)
        entries = result["data"]["workspace"]["workspaceEntries"]

        # Create mapping of
        # - location name => deployment metadata
        deployment_map: Dict[str, UserCodeLauncherEntry] = {}
        upload_locations = set()
        for entry in entries:
            location_name = entry["locationName"]
            deployment_metadata = deserialize_as(
                entry["serializedDeploymentMetadata"], CodeDeploymentMetadata
            )
            sandbox_saved_timestamp = (
                float(entry["sandboxSavedTimestamp"])
                if entry.get("sandboxSavedTimestamp")
                else None
            )
            sandbox_proxy_info = entry["sandboxProxyInfo"]
            if sandbox_proxy_info:
                sandbox_proxy_info = DagsterCloudSandboxProxyInfo(
                    hostname=sandbox_proxy_info.get("hostname"),
                    port=sandbox_proxy_info.get("port"),
                    auth_token=sandbox_proxy_info.get("authToken"),
                    min_port=sandbox_proxy_info.get("minPort"),
                    max_port=sandbox_proxy_info.get("maxPort"),
                )
            deployment_map[location_name] = UserCodeLauncherEntry(
                code_deployment_metadata=deployment_metadata,
                update_timestamp=float(entry["metadataTimestamp"]),
                sandbox_saved_timestamp=sandbox_saved_timestamp,
                sandbox_proxy_info=sandbox_proxy_info,
            )
            if upload_results and entry["hasOutdatedData"]:
                upload_locations.add(location_name)

        user_code_launcher.update_grpc_metadata(deployment_map, upload_locations)

    def _get_grpc_client(
        self,
        user_code_launcher: DagsterCloudUserCodeLauncher,
        repository_location_origin: RepositoryLocationOrigin,
    ) -> DagsterGrpcClient:
        endpoint = user_code_launcher.get_grpc_endpoint(repository_location_origin)
        if user_code_launcher.is_dev_sandbox():
            # Retry client construction on sandboxes because their gRPC servers
            # have brief downtimes while soft reloading.
            client = backoff.backoff(
                fn=endpoint.create_client,
                retry_on=(DagsterUserCodeUnreachableError,),
                max_retries=5,
            )
        else:
            client = endpoint.create_client()
        return client

    def _handle_api_request(
        self,
        request: DagsterCloudApiRequest,
        instance: DagsterCloudAgentInstance,
        user_code_launcher: DagsterCloudUserCodeLauncher,
    ) -> Union[DagsterCloudApiSuccess, DagsterCloudApiGrpcResponse]:
        api_name = request.request_api
        if api_name == DagsterCloudApi.CHECK_FOR_WORKSPACE_UPDATES:
            # Dagster Cloud has requested that we upload new metadata for any out of date locations in
            # the workspace
            self._query_for_workspace_updates(instance, user_code_launcher, upload_results=True)
            return DagsterCloudApiSuccess()
        elif api_name == DagsterCloudApi.GET_EXTERNAL_EXECUTION_PLAN:
            external_pipeline_origin = request.request_args.pipeline_origin
            client = self._get_grpc_client(
                user_code_launcher,
                external_pipeline_origin.external_repository_origin.repository_location_origin,
            )
            serialized_snapshot_or_error = client.execution_plan_snapshot(
                execution_plan_snapshot_args=request.request_args
            )
            return DagsterCloudApiGrpcResponse(serialized_snapshot_or_error)

        elif api_name == DagsterCloudApi.GET_SUBSET_EXTERNAL_PIPELINE_RESULT:
            external_pipeline_origin = request.request_args.pipeline_origin
            client = self._get_grpc_client(
                user_code_launcher,
                external_pipeline_origin.external_repository_origin.repository_location_origin,
            )

            serialized_subset_result_or_error = client.external_pipeline_subset(
                pipeline_subset_snapshot_args=request.request_args
            )

            return DagsterCloudApiGrpcResponse(serialized_subset_result_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_PARTITION_CONFIG:
            external_repository_origin = request.request_args.repository_origin
            client = self._get_grpc_client(
                user_code_launcher, external_repository_origin.repository_location_origin
            )
            serialized_partition_config_or_error = client.external_partition_config(
                partition_args=request.request_args,
            )
            return DagsterCloudApiGrpcResponse(serialized_partition_config_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_PARTITION_TAGS:
            external_repository_origin = request.request_args.repository_origin
            client = self._get_grpc_client(
                user_code_launcher, external_repository_origin.repository_location_origin
            )
            serialized_partition_tags_or_error = client.external_partition_tags(
                partition_args=request.request_args,
            )
            return DagsterCloudApiGrpcResponse(serialized_partition_tags_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_PARTITION_NAMES:
            external_repository_origin = request.request_args.repository_origin
            client = self._get_grpc_client(
                user_code_launcher, external_repository_origin.repository_location_origin
            )
            serialized_partition_names_or_error = client.external_partition_names(
                partition_names_args=request.request_args,
            )
            return DagsterCloudApiGrpcResponse(serialized_partition_names_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_PARTITION_SET_EXECUTION_PARAM_DATA:
            external_repository_origin = request.request_args.repository_origin
            client = self._get_grpc_client(
                user_code_launcher, external_repository_origin.repository_location_origin
            )
            serialized_partition_execution_params_or_error = (
                client.external_partition_set_execution_params(
                    partition_set_execution_param_args=request.request_args
                )
            )
            return DagsterCloudApiGrpcResponse(serialized_partition_execution_params_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_SCHEDULE_EXECUTION_DATA:
            external_repository_origin = request.request_args.repository_origin
            client = self._get_grpc_client(
                user_code_launcher, external_repository_origin.repository_location_origin
            )

            args = request.request_args._replace(instance_ref=instance.get_ref())

            serialized_schedule_data_or_error = client.external_schedule_execution(
                external_schedule_execution_args=args,
            )

            return DagsterCloudApiGrpcResponse(serialized_schedule_data_or_error)

        elif api_name == DagsterCloudApi.GET_EXTERNAL_SENSOR_EXECUTION_DATA:
            external_repository_origin = request.request_args.repository_origin
            client = self._get_grpc_client(
                user_code_launcher, external_repository_origin.repository_location_origin
            )

            args = request.request_args._replace(instance_ref=instance.get_ref())

            serialized_sensor_data_or_error = client.external_sensor_execution(
                sensor_execution_args=args,
            )

            return DagsterCloudApiGrpcResponse(serialized_sensor_data_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_NOTEBOOK_DATA:
            client = self._get_grpc_client(
                user_code_launcher, request.request_args.repository_location_origin
            )
            response = client.external_notebook_data(request.request_args.notebook_path)
            return DagsterCloudApiGrpcResponse(response.decode())
        elif api_name == DagsterCloudApi.LAUNCH_RUN:
            run = request.request_args.pipeline_run

            instance.report_engine_event(
                f"{instance.agent_display_name} is launching run {run.run_id}",
                run,
                cls=self.__class__,
            )

            instance.add_run_tags(
                run.run_id,
                merge_dicts(
                    {"dagster/agent_label": instance.dagster_cloud_api_agent_label}
                    if instance.dagster_cloud_api_agent_label
                    else {},
                    {"dagster/agent_id": instance.instance_uuid},
                ),
            )

            if user_code_launcher.is_dev_sandbox():
                repository_location_origin = (
                    run.external_pipeline_origin.external_repository_origin.repository_location_origin
                )

                client = self._get_grpc_client(
                    user_code_launcher,
                    repository_location_origin,
                )
                DefaultRunLauncher.launch_run_from_grpc_client(
                    instance=instance,
                    run=run,
                    grpc_client=client,
                )
            else:
                launcher = user_code_launcher.run_launcher()
                launcher.launch_run(LaunchRunContext(pipeline_run=run, workspace=None))
            return DagsterCloudApiSuccess()
        elif api_name == DagsterCloudApi.TERMINATE_RUN:
            # With agent replicas enabled:
            # Run workers now poll for run status. We don't use the run launcher to terminate.
            # Once min agent version is bumped, we can deprecate this command.
            # For backcompat, we use the run launcher to terminate unless the user opts in.
            run = request.request_args.pipeline_run
            if instance.agent_replicas_enabled:
                instance.report_engine_event(
                    f"{instance.agent_display_name} received request to mark run as canceling",
                    run,
                    cls=self.__class__,
                )
                instance.report_run_canceling(run)
            else:
                instance.report_engine_event(
                    f"{instance.agent_display_name} received request to terminate run",
                    run,
                    cls=self.__class__,
                )
                if user_code_launcher.is_dev_sandbox():
                    launcher = DefaultRunLauncher()
                    launcher.register_instance(instance)
                else:
                    launcher = user_code_launcher.run_launcher()
                launcher.terminate(run.run_id)
            return DagsterCloudApiSuccess()

        else:
            check.assert_never(api_name)
            raise Exception(
                "Unexpected dagster cloud api call {api_name}".format(api_name=api_name)
            )

    def _process_api_request(
        self,
        json_request: Dict,
        instance: DagsterCloudAgentInstance,
        user_code_launcher: DagsterCloudUserCodeLauncher,
        submitted_to_executor_timestamp: float,
    ) -> Optional[SerializableErrorInfo]:
        thread_start_run_timestamp = pendulum.now("UTC").timestamp()
        api_result: Optional[DagsterCloudApiResponse] = None
        error_info: Optional[SerializableErrorInfo] = None

        request_id = json_request["requestId"]
        request_api = json_request["requestApi"]
        request_body = json_request["requestBody"]

        request: Union[str, DagsterCloudApiRequest] = DagsterCloudApiRequest.format_request(
            request_id, request_api
        )

        if request_api not in DagsterCloudApi.__members__:
            api_result = DagsterCloudApiUnknownCommandResponse(request_api)
            self._logger.warning(
                "Ignoring request {request}: Unknown command. This is likely due to running an "
                "older version of the agent.".format(request=json_request)
            )
        else:
            try:
                request = cast(
                    DagsterCloudApiRequest, deserialize_json_to_dagster_namedtuple(request_body)
                )
                self._logger.info(
                    "Received request {request}.".format(
                        request=request,
                    )
                )
                api_result = self._handle_api_request(request, instance, user_code_launcher)
            except Exception:
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                api_result = DagsterCloudApiErrorResponse(error_infos=[error_info])

                self._logger.error(
                    "Error serving request {request}: {error_info}".format(
                        request=json_request,
                        error_info=error_info,
                    )
                )

        self._logger.info(
            "Finished processing request {request}.".format(
                request=request,
            )
        )

        thread_finished_request_time = pendulum.now("UTC").timestamp()
        thread_telemetry = DagsterCloudApiThreadTelemetry(
            submitted_to_executor_timestamp=submitted_to_executor_timestamp,
            thread_start_run_timestamp=thread_start_run_timestamp,
            thread_end_handle_api_request_timestamp=thread_finished_request_time,
        )

        api_result = api_result.with_thread_telemetry(thread_telemetry)

        assert api_result

        upload_response = DagsterCloudUploadApiResponse(
            request_id=request_id, request_api=request_api, response=api_result
        )

        self._logger.info(
            "Uploading response for request {request}.".format(
                request=request,
            )
        )

        upload_api_response(instance, upload_response)

        self._logger.info(
            "Finished uploading response for request {request}.".format(
                request=request,
            )
        )

        return error_info

    def run_iteration(
        self, instance: DagsterCloudAgentInstance, user_code_launcher: DagsterCloudUserCodeLauncher
    ) -> Iterator[Optional[SerializableErrorInfo]]:
        result = instance.graphql_client.execute(GET_USER_CLOUD_REQUESTS_QUERY)
        json_requests = result["data"]["userCloudAgent"]["popUserCloudAgentRequests"]

        self._logger.debug(
            "Iteration #{iteration}: Adding {num_requests} requests to process.".format(
                iteration=self._iteration, num_requests=len(json_requests)
            )
        )

        # Submit requests to threadpool and store the futures
        for json_request in json_requests:
            request_id = json_request["requestId"]

            submitted_to_executor_timestamp = pendulum.now("UTC").timestamp()
            future_context = DagsterCloudApiFutureContext(
                future=self._executor.submit(
                    self._process_api_request,
                    json_request,
                    instance,
                    user_code_launcher,
                    submitted_to_executor_timestamp,
                ),
            )

            self._request_ids_to_future_context[request_id] = future_context

        # Process futures that are done or have timed out
        # Create a shallow copy of the futures dict to modify it while iterating
        for request_id, future_context in self._request_ids_to_future_context.copy().items():
            if future_context.future.done() or future_context.timed_out():
                response: Optional[SerializableErrorInfo] = None

                try:
                    response = future_context.future.result(timeout=0)
                except:
                    response = serializable_error_info_from_exc_info(sys.exc_info())

                # Do not process a request again once we have its result
                del self._request_ids_to_future_context[request_id]

                # Yield the error information from the future
                if response:
                    yield response

        self._iteration += 1

        yield None


def upload_api_response(
    instance: DagsterCloudAgentInstance, upload_response: DagsterCloudUploadApiResponse
):
    with tempfile.TemporaryDirectory() as temp_dir:
        dst = os.path.join(temp_dir, "api_response.tmp")
        with open(dst, "wb") as f:
            f.write(zlib.compress(serialize_dagster_namedtuple(upload_response).encode("utf-8")))

        with open(dst, "rb") as f:
            resp = instance.requests_session.post(
                instance.dagster_cloud_upload_api_response_url,
                headers=instance.dagster_cloud_api_headers,
                files={"api_response.tmp": f},
                timeout=instance.dagster_cloud_api_timeout,
            )
            raise_http_error(resp)
