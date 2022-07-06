import concurrent.futures
import logging
import os
import sys
import tempfile
import time
import zlib
from collections import deque
from contextlib import ExitStack
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Set, Tuple, Union, cast

import dagster._check as check
import pendulum
from dagster import DagsterInstance
from dagster.core.host_representation import RepositoryLocationOrigin
from dagster.core.host_representation.origin import RegisteredRepositoryLocationOrigin
from dagster.core.launcher.base import LaunchRunContext
from dagster.grpc.client import DagsterGrpcClient
from dagster.serdes import (
    deserialize_as,
    deserialize_json_to_dagster_namedtuple,
    serialize_dagster_namedtuple,
)
from dagster.utils import merge_dicts
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
    DagsterCloudUploadApiResponse,
    TimestampedError,
)
from dagster_cloud.instance import DagsterCloudAgentInstance
from dagster_cloud.workspace.user_code_launcher import (
    DagsterCloudUserCodeLauncher,
    UserCodeLauncherEntry,
)
from dagster_cloud_cli.core.errors import GraphQLStorageError, raise_http_error
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata

from ..version import __version__
from .queries import (
    ADD_AGENT_HEARTBEATS_MUTATION,
    ADD_AGENT_HEARTBEAT_MUTATION,
    GET_USER_CLOUD_REQUESTS_QUERY,
    WORKSPACE_ENTRIES_QUERY,
)

CHECK_WORKSPACE_INTERVAL_SECONDS = 5

# Interval at which Agent heartbeats are sent to the host cloud
AGENT_HEARTBEAT_INTERVAL_SECONDS = 30

AGENT_HEARTBEAT_ERROR_LIMIT = 25  # Send at most 25 errors

DEFAULT_PENDING_REQUESTS_LIMIT = 100

DEPLOYMENT_INFO_QUERY = """
    query DeploymentInfo {
         deploymentInfo {
             deploymentName
         }
     }
"""


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

    def __init__(self, pending_requests_limit: int = DEFAULT_PENDING_REQUESTS_LIMIT):
        self._logger = logging.getLogger("dagster_cloud")

        self._logger.info("Starting Dagster Cloud agent...")

        self._exit_stack = ExitStack()
        self._iteration = 0

        max_workers = (os.cpu_count() or 1) * self.MAX_THREADS_PER_CORE
        self._executor = self._exit_stack.enter_context(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="dagster_cloud_agent_worker",
            )
        )

        self._request_ids_to_future_context: Dict[str, DagsterCloudApiFutureContext] = {}

        self._last_heartbeat_time = None

        self._last_workspace_check_time = None

        self._errors: deque = deque(
            maxlen=AGENT_HEARTBEAT_ERROR_LIMIT
        )  # (SerializableErrorInfo, timestamp) tuples

        self._pending_requests: List[Dict[str, Any]] = []
        self._locations_with_pending_requests: Set[Tuple[str, str]] = set()
        self._ready_requests: List[Dict[str, Any]] = []

        self._location_query_times: Dict[Tuple[str, str], float] = {}
        self._pending_requests_limit = check.int_param(
            pending_requests_limit, "pending_requests_limit"
        )
        self._active_deployments: Set[str] = set()

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception_value, _traceback):
        self._exit_stack.close()

    def run_loop(self, instance, user_code_launcher, agent_uuid):
        heartbeat_interval_seconds = AGENT_HEARTBEAT_INTERVAL_SECONDS

        if not user_code_launcher.server_ttl_enabled:

            if not instance.deployment_name:
                self._logger.info(
                    "Deployment name was not set - checking to see if it can be fetched from the server..."
                )
                # Fetch the deployment name from the server if it isn't set (only true
                # for old agents, and only will work if there's a single deployment in the org)
                result = instance.graphql_client.execute(DEPLOYMENT_INFO_QUERY)
                deployment_name = result["data"]["deploymentInfo"]["deploymentName"]
                instance = self._exit_stack.enter_context(
                    DagsterInstance.from_ref(instance.ref_for_deployment(deployment_name))
                )

            self._check_update_workspace(instance, user_code_launcher)

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
        self._query_for_workspace_updates(instance, user_code_launcher)

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

        if instance.for_branch_deployments:
            res = instance.graphql_client.execute(
                ADD_AGENT_HEARTBEATS_MUTATION,
                variable_values={
                    "serializedAgentHeartbeat": serialize_dagster_namedtuple(heartbeat),
                    "deploymentNames": list(self._active_deployments),
                },
            )
        else:
            res = instance.graphql_client.execute(
                ADD_AGENT_HEARTBEAT_MUTATION,
                variable_values={
                    "serializedAgentHeartbeat": serialize_dagster_namedtuple(heartbeat)
                },
            )
        if "errors" in res:
            raise GraphQLStorageError(res)

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        return self._executor

    @property
    def request_ids_to_future_context(self) -> Dict[str, DagsterCloudApiFutureContext]:
        return self._request_ids_to_future_context

    def _upload_outdated_workspace_entries(
        self,
        instance: DagsterCloudAgentInstance,
        deployment_name: str,
        user_code_launcher: DagsterCloudUserCodeLauncher,
    ):
        result = instance.graphql_client_for_deployment(deployment_name).execute(
            WORKSPACE_ENTRIES_QUERY,
            variable_values={"deploymentNames": [deployment_name]},
        )
        entries = result["data"]["deployments"][0]["workspaceEntries"]
        now = time.time()

        upload_metadata = {}

        for entry in entries:
            location_name = entry["locationName"]
            deployment_metadata = deserialize_as(
                entry["serializedDeploymentMetadata"], CodeDeploymentMetadata
            )
            if entry["hasOutdatedData"]:
                # Spin up a server for this location and upload its metadata to Cloud
                # (Bump the TTL counter as well to leave the server up)
                self._location_query_times[(deployment_name, location_name)] = now
                upload_metadata[(deployment_name, location_name)] = UserCodeLauncherEntry(
                    code_deployment_metadata=deployment_metadata,
                    update_timestamp=float(entry["metadataTimestamp"]),
                )

        user_code_launcher.add_upload_metadata(upload_metadata)

    def _query_for_workspace_updates(
        self,
        instance: DagsterCloudAgentInstance,
        user_code_launcher: DagsterCloudUserCodeLauncher,
    ):

        now = time.time()

        if user_code_launcher.server_ttl_enabled:
            # Include the location if:
            # - There's a pending request in the queue for it
            # - Its TTL hasn't expired since the last time somebody asked for it
            locations_to_consider = {
                deployment_and_location
                for deployment_and_location in self._locations_with_pending_requests
            }.union(
                {
                    deployment_and_location
                    for deployment_and_location in self._location_query_times
                    if (
                        now - self._location_query_times[deployment_and_location]
                        < user_code_launcher.server_ttl_seconds
                    )
                }
            )
            deployments_to_consider = {key[0] for key in locations_to_consider}
        else:
            deployments_to_consider = {cast(str, instance.deployment_name)}
            locations_to_consider = None  # All locations from this deployment are loaded
        self._active_deployments = deployments_to_consider

        # Create mapping of
        # - location name => deployment metadata
        deployment_map: Dict[Tuple[str, str], UserCodeLauncherEntry] = {}
        all_locations: Set[Tuple[str, str]] = set()

        if deployments_to_consider:
            result = instance.graphql_client.execute(
                WORKSPACE_ENTRIES_QUERY,
                variable_values={"deploymentNames": list(deployments_to_consider)},
            )

            for deployment_result in result["data"]["deployments"]:
                deployment_name = deployment_result["deploymentName"]

                entries = deployment_result["workspaceEntries"]

                for entry in entries:
                    location_name = entry["locationName"]

                    location_key = (deployment_name, location_name)

                    all_locations.add(location_key)
                    deployment_metadata = deserialize_as(
                        entry["serializedDeploymentMetadata"], CodeDeploymentMetadata
                    )

                    if not user_code_launcher.server_ttl_enabled or location_key in cast(
                        Set[Tuple[str, str]], locations_to_consider
                    ):
                        deployment_map[location_key] = UserCodeLauncherEntry(
                            code_deployment_metadata=deployment_metadata,
                            update_timestamp=float(entry["metadataTimestamp"]),
                        )

        user_code_launcher.update_grpc_metadata(deployment_map)

        # In the rare event that there are pending requests that are no longer in the workspace at
        # all (if, say, a location is removed while requests are enqueued), they should be forcibly
        # moved to ready so that they don't stay pending forever - callsites will get an error
        # about the location not existing, but that's preferable to slowly timing out
        pending_requests_copy = self._pending_requests.copy()
        self._pending_requests = []
        for json_request in pending_requests_copy:
            location_name = self._get_location_from_request(json_request)
            deployment_name = json_request["deploymentName"]
            if (deployment_name, location_name) not in all_locations:
                self._ready_requests.append(json_request)
            else:
                self._pending_requests.append(json_request)

    def _get_grpc_client(
        self,
        user_code_launcher: DagsterCloudUserCodeLauncher,
        deployment_name: str,
        location_name: str,
    ) -> DagsterGrpcClient:
        endpoint = user_code_launcher.get_grpc_endpoint(deployment_name, location_name)
        return endpoint.create_client()

    def _get_location_origin_from_request(
        self,
        request: DagsterCloudApiRequest,
    ) -> Optional[RepositoryLocationOrigin]:
        """Derive the location from the specific argument passed in to a dagster_cloud_api call."""
        api_name = request.request_api
        if api_name in {
            DagsterCloudApi.GET_EXTERNAL_EXECUTION_PLAN,
            DagsterCloudApi.GET_SUBSET_EXTERNAL_PIPELINE_RESULT,
        }:
            external_pipeline_origin = request.request_args.pipeline_origin
            return external_pipeline_origin.external_repository_origin.repository_location_origin
        elif api_name in {
            DagsterCloudApi.GET_EXTERNAL_PARTITION_CONFIG,
            DagsterCloudApi.GET_EXTERNAL_PARTITION_TAGS,
            DagsterCloudApi.GET_EXTERNAL_PARTITION_NAMES,
            DagsterCloudApi.GET_EXTERNAL_PARTITION_SET_EXECUTION_PARAM_DATA,
            DagsterCloudApi.GET_EXTERNAL_SCHEDULE_EXECUTION_DATA,
            DagsterCloudApi.GET_EXTERNAL_SENSOR_EXECUTION_DATA,
        }:
            return request.request_args.repository_origin.repository_location_origin
        elif api_name == DagsterCloudApi.GET_EXTERNAL_NOTEBOOK_DATA:
            return request.request_args.repository_location_origin
        elif api_name == DagsterCloudApi.PING_LOCATION:
            return RegisteredRepositoryLocationOrigin(request.request_args.location_name)
        else:
            return None

    def _handle_api_request(
        self,
        request: DagsterCloudApiRequest,
        deployment_name: str,
        instance: DagsterCloudAgentInstance,
        user_code_launcher: DagsterCloudUserCodeLauncher,
    ) -> Union[DagsterCloudApiSuccess, DagsterCloudApiGrpcResponse]:
        api_name = request.request_api

        repository_location_origin = self._get_location_origin_from_request(request)
        location_name = (
            repository_location_origin.location_name if repository_location_origin else None
        )

        if api_name == DagsterCloudApi.PING_LOCATION:
            # Do nothing - this request only exists to bump TTL for the location
            return DagsterCloudApiSuccess()
        elif api_name == DagsterCloudApi.CHECK_FOR_WORKSPACE_UPDATES:
            # Dagster Cloud has requested that we upload new metadata for any out of date locations in
            # the workspace
            self._upload_outdated_workspace_entries(instance, deployment_name, user_code_launcher)
            return DagsterCloudApiSuccess()
        elif api_name == DagsterCloudApi.GET_EXTERNAL_EXECUTION_PLAN:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )
            serialized_snapshot_or_error = client.execution_plan_snapshot(
                execution_plan_snapshot_args=request.request_args._replace(
                    instance_ref=instance.ref_for_deployment(deployment_name)
                )
            )
            return DagsterCloudApiGrpcResponse(serialized_snapshot_or_error)

        elif api_name == DagsterCloudApi.GET_SUBSET_EXTERNAL_PIPELINE_RESULT:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )

            serialized_subset_result_or_error = client.external_pipeline_subset(
                pipeline_subset_snapshot_args=request.request_args
            )

            return DagsterCloudApiGrpcResponse(serialized_subset_result_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_PARTITION_CONFIG:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )
            serialized_partition_config_or_error = client.external_partition_config(
                partition_args=request.request_args,
            )
            return DagsterCloudApiGrpcResponse(serialized_partition_config_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_PARTITION_TAGS:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )
            serialized_partition_tags_or_error = client.external_partition_tags(
                partition_args=request.request_args,
            )
            return DagsterCloudApiGrpcResponse(serialized_partition_tags_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_PARTITION_NAMES:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )
            serialized_partition_names_or_error = client.external_partition_names(
                partition_names_args=request.request_args,
            )
            return DagsterCloudApiGrpcResponse(serialized_partition_names_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_PARTITION_SET_EXECUTION_PARAM_DATA:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )
            serialized_partition_execution_params_or_error = (
                client.external_partition_set_execution_params(
                    partition_set_execution_param_args=request.request_args
                )
            )
            return DagsterCloudApiGrpcResponse(serialized_partition_execution_params_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_SCHEDULE_EXECUTION_DATA:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )

            args = request.request_args._replace(
                instance_ref=instance.ref_for_deployment(deployment_name)
            )

            serialized_schedule_data_or_error = client.external_schedule_execution(
                external_schedule_execution_args=args,
            )

            return DagsterCloudApiGrpcResponse(serialized_schedule_data_or_error)

        elif api_name == DagsterCloudApi.GET_EXTERNAL_SENSOR_EXECUTION_DATA:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )

            args = request.request_args._replace(
                instance_ref=instance.ref_for_deployment(deployment_name)
            )

            serialized_sensor_data_or_error = client.external_sensor_execution(
                sensor_execution_args=args,
            )

            return DagsterCloudApiGrpcResponse(serialized_sensor_data_or_error)
        elif api_name == DagsterCloudApi.GET_EXTERNAL_NOTEBOOK_DATA:
            client = self._get_grpc_client(
                user_code_launcher, deployment_name, cast(str, location_name)
            )
            response = client.external_notebook_data(request.request_args.notebook_path)
            return DagsterCloudApiGrpcResponse(response.decode())
        elif api_name == DagsterCloudApi.LAUNCH_RUN:
            run = request.request_args.pipeline_run

            with DagsterInstance.from_ref(
                instance.ref_for_deployment(deployment_name)
            ) as scoped_instance:
                scoped_instance.report_engine_event(
                    f"{instance.agent_display_name} is launching run {run.run_id}",
                    run,
                    cls=self.__class__,
                )

                scoped_instance.add_run_tags(
                    run.run_id,
                    merge_dicts(
                        {"dagster/agent_label": instance.dagster_cloud_api_agent_label}
                        if instance.dagster_cloud_api_agent_label
                        else {},
                        {"dagster/agent_id": instance.instance_uuid},
                    ),
                )

                scoped_instance.run_launcher.launch_run(
                    LaunchRunContext(pipeline_run=run, workspace=None)
                )
                return DagsterCloudApiSuccess()
        elif api_name == DagsterCloudApi.TERMINATE_RUN:
            # With agent replicas enabled:
            # Run workers now poll for run status. We don't use the run launcher to terminate.
            # Once min agent version is bumped, we can deprecate this command.
            # For backcompat, we use the run launcher to terminate unless the user opts in.
            run = request.request_args.pipeline_run

            with DagsterInstance.from_ref(
                instance.ref_for_deployment(deployment_name)
            ) as scoped_instance:
                if instance.agent_replicas_enabled:
                    scoped_instance.report_engine_event(
                        f"{instance.agent_display_name} received request to mark run as canceling",
                        run,
                        cls=self.__class__,
                    )
                    scoped_instance.report_run_canceling(run)
                else:
                    scoped_instance.report_engine_event(
                        f"{instance.agent_display_name} received request to terminate run",
                        run,
                        cls=self.__class__,
                    )
                    scoped_instance.run_launcher.terminate(run.run_id)
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
        deployment_name = json_request["deploymentName"]

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
                api_result = self._handle_api_request(
                    request, deployment_name, instance, user_code_launcher
                )
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

        upload_api_response(instance, deployment_name, upload_response)

        self._logger.info(
            "Finished uploading response for request {request}.".format(
                request=request,
            )
        )

        return error_info

    def _get_location_from_request(self, json_request: Dict[str, Any]) -> Optional[str]:
        request_api = json_request["requestApi"]
        request_body = json_request["requestBody"]
        if request_api not in DagsterCloudApi.__members__:
            return None

        request = cast(DagsterCloudApiRequest, deserialize_json_to_dagster_namedtuple(request_body))
        location_origin = self._get_location_origin_from_request(request)
        if not location_origin:
            return None

        return location_origin.location_name

    def run_iteration(
        self, instance: DagsterCloudAgentInstance, user_code_launcher: DagsterCloudUserCodeLauncher
    ) -> Iterator[Optional[SerializableErrorInfo]]:

        num_pending_requests = len(self._pending_requests)

        if num_pending_requests < self._pending_requests_limit:
            result = instance.graphql_client.execute(
                GET_USER_CLOUD_REQUESTS_QUERY,
                {"forBranchDeployments": instance.for_branch_deployments},
            )
            json_requests = result["data"]["userCloudAgent"]["popUserCloudAgentRequests"]

            self._logger.debug(
                "Iteration #{iteration}: Adding {num_requests} requests to be processed. Currently {num_pending_requests} waiting for server to be ready".format(
                    iteration=self._iteration,
                    num_requests=len(json_requests),
                    num_pending_requests=num_pending_requests,
                )
            )

            self._pending_requests.extend(json_requests)
        else:
            self._logger.warning(
                "Iteration #{iteration}: Waiting to pull requests from the queue since there are already {num_pending_requests} in the queue".format(
                    iteration=self._iteration,
                    num_pending_requests=len(self._pending_requests),
                )
            )

        invalid_requests = []
        self._locations_with_pending_requests = set()

        # Determine which pending requests are now ready (their locations have been loaded, or the
        # request does not correspond to a particular location)
        for json_request in self._pending_requests:
            deployment_name = json_request["deploymentName"]
            location_name = self._get_location_from_request(json_request)
            if location_name:
                self._location_query_times[(deployment_name, location_name)] = time.time()

                if not user_code_launcher.has_grpc_endpoint(deployment_name, location_name):
                    # Next completed periodic workspace update will make the location up to date
                    # - keep this in the queue until then
                    invalid_requests.append(json_request)
                    self._locations_with_pending_requests.add((deployment_name, location_name))
                    continue

            self._ready_requests.append(json_request)

        # Any invalid requests go back in the pending queue - the next workspace update will
        # ensure that the usercodelauncher spins up locations for those requests
        self._pending_requests = invalid_requests

        # send all ready requests to the threadpool
        for json_request in self._ready_requests:
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

        self._ready_requests = []

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
    instance: DagsterCloudAgentInstance,
    deployment_name: str,
    upload_response: DagsterCloudUploadApiResponse,
):
    with tempfile.TemporaryDirectory() as temp_dir:
        dst = os.path.join(temp_dir, "api_response.tmp")
        with open(dst, "wb") as f:
            f.write(zlib.compress(serialize_dagster_namedtuple(upload_response).encode("utf-8")))

        with open(dst, "rb") as f:
            resp = instance.requests_session.post(
                instance.dagster_cloud_upload_api_response_url,
                headers=instance.headers_for_deployment(deployment_name),
                files={"api_response.tmp": f},
                timeout=instance.dagster_cloud_api_timeout,
            )
            raise_http_error(resp)
