import concurrent.futures
import logging
import os
import sys
import time
from collections import deque
from contextlib import ExitStack
from typing import Dict, Iterator, List, NamedTuple, Optional, Union

import pendulum
from dagster import check
from dagster.core.events import DagsterEvent
from dagster.core.events.log import EventLogEntry
from dagster.core.host_representation import RepositoryLocationOrigin
from dagster.core.launcher.base import LaunchRunContext
from dagster.core.workspace.dynamic_workspace import DynamicWorkspace
from dagster.daemon.controller import (
    DEFAULT_DAEMON_ERROR_INTERVAL_SECONDS,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
)
from dagster.daemon.daemon import (
    DAEMON_HEARTBEAT_ERROR_LIMIT,
    DagsterDaemon,
    get_default_daemon_logger,
)
from dagster.daemon.types import DaemonHeartbeat
from dagster.grpc.client import DagsterGrpcClient
from dagster.serdes import deserialize_json_to_dagster_namedtuple, serialize_dagster_namedtuple
from dagster.utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster.utils.interrupts import raise_interrupts_as
from dagster_cloud.api.dagster_cloud_api import (
    DagsterCloudApi,
    DagsterCloudApiErrorResponse,
    DagsterCloudApiGrpcResponse,
    DagsterCloudApiRequest,
    DagsterCloudApiResponse,
    DagsterCloudApiSuccess,
    DagsterCloudApiThreadTelemetry,
    DagsterCloudApiUnknownCommandResponse,
)
from dagster_cloud.execution.watchful_run_launcher.base import WatchfulRunLauncher
from dagster_cloud.executor.step_handler_context import DagsterCloudStepHandlerContext
from dagster_cloud.instance import DagsterCloudAgentInstance
from dagster_cloud.workspace.origin import CodeDeploymentMetadata
from dagster_cloud.workspace.user_code_launcher import DagsterCloudUserCodeLauncher

from .queries import (
    GET_USER_CLOUD_REQUESTS_QUERY,
    SEND_USER_CLOUD_RESPONSE_MUTATION,
    WORKSPACE_ENTRIES_QUERY,
)


class DagsterCloudApiFutureContext(
    NamedTuple(
        "_DagsterCloudApiFutureContext",
        [
            ("future", concurrent.futures.Future),
            ("timeout", float),
        ],
    )
):
    DEFAULT_TIMEOUT_SECONDS = 15

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


# Stub class to ensure that dagit still displays the status of the agent
# (Even though the actual implementation of the agent no longer uses
# the DagsterDaemon base class)
class DagsterCloudApiDaemon(DagsterDaemon):
    def __init__(self):
        super(DagsterCloudApiDaemon, self).__init__(interval_seconds=0.5)

    def run_iteration(
        self, instance: DagsterCloudAgentInstance, workspace: DynamicWorkspace
    ) -> Iterator[Optional[SerializableErrorInfo]]:
        raise NotImplementedError

    @classmethod
    def daemon_type(cls) -> str:
        return "DAGSTER_CLOUD_API"


class DagsterCloudApiAgent:
    MAX_THREADS_PER_CORE = 10

    def __init__(self):
        self._logger = get_default_daemon_logger(type(self).__name__)

        self._exit_stack = ExitStack()
        self._initial_workspace_loaded = False
        self._iteration = 0

        max_workers = os.cpu_count() * self.MAX_THREADS_PER_CORE
        self._executor = self._exit_stack.enter_context(
            concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        )

        self._request_ids_to_future_context: Dict[str, DagsterCloudApiFutureContext] = {}

        self._last_heartbeat_time = None
        self._errors = deque(
            maxlen=DAEMON_HEARTBEAT_ERROR_LIMIT
        )  # (SerializableErrorInfo, timestamp) tuples

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception_value, _traceback):
        self._exit_stack.close()

    def run_loop(self, instance, user_code_launcher, agent_uuid):
        heartbeat_interval_seconds = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
        error_interval_seconds = DEFAULT_DAEMON_ERROR_INTERVAL_SECONDS

        while True:
            try:
                for error in self.run_iteration(instance, user_code_launcher):
                    if error:
                        self._errors.appendleft((error, pendulum.now("UTC")))
            except Exception:  # pylint: disable=broad-except
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error("Caught error:\n{}".format(error_info))
                self._errors.appendleft((error_info, pendulum.now("UTC")))
            try:
                self._check_add_heartbeat(
                    instance,
                    agent_uuid,
                    heartbeat_interval_seconds,
                    error_interval_seconds,
                )
            except Exception:  # pylint: disable=broad-except
                self._logger.error(
                    "Failed to add heartbeat: \n{}".format(
                        serializable_error_info_from_exc_info(sys.exc_info())
                    )
                )

            time.sleep(0.5)

            # Check for any received interrupts
            with raise_interrupts_as(KeyboardInterrupt):
                pass

    def _check_add_heartbeat(
        self, instance, agent_uuid, heartbeat_interval_seconds, error_interval_seconds
    ):
        error_max_time = pendulum.now("UTC").subtract(seconds=error_interval_seconds)

        while len(self._errors):
            _earliest_error, earliest_timestamp = self._errors[-1]
            if earliest_timestamp >= error_max_time:
                break
            self._errors.pop()

        curr_time = pendulum.now("UTC")

        if (
            self._last_heartbeat_time
            and (curr_time - self._last_heartbeat_time).total_seconds() < heartbeat_interval_seconds
        ):
            return

        daemon_type = DagsterCloudApiDaemon.daemon_type()

        last_stored_heartbeat = instance.get_daemon_heartbeats().get(daemon_type)
        if (
            self._last_heartbeat_time
            and last_stored_heartbeat
            and last_stored_heartbeat.daemon_id != agent_uuid
        ):
            self._logger.warning(
                "Taking over from another {} agent process. If this "
                "message reoccurs, you may have multiple agents running which is not supported. "
                "Last heartbeat agent id: {}, "
                "Current agent ID: {}".format(
                    daemon_type,
                    last_stored_heartbeat.daemon_id,
                    agent_uuid,
                )
            )

        self._last_heartbeat_time = curr_time

        instance.add_daemon_heartbeat(
            DaemonHeartbeat(
                curr_time.float_timestamp,
                daemon_type,
                agent_uuid,
                errors=[error for (error, timestamp) in self._errors],
            )
        )

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        return self._executor

    @property
    def request_ids_to_future_context(self) -> Dict[str, DagsterCloudApiFutureContext]:
        return self._request_ids_to_future_context

    def _check_for_workspace_updates(
        self, instance: DagsterCloudAgentInstance, user_code_launcher: DagsterCloudUserCodeLauncher
    ):
        # Get list of workspace entries from DB
        result = instance.graphql_client.execute(WORKSPACE_ENTRIES_QUERY)
        entries = result["data"]["workspace"]["workspaceEntries"]

        # Create mapping of
        # - location name => deployment metadata
        deployment_map: Dict[str, CodeDeploymentMetadata] = {}
        force_update_locations = set()
        for entry in entries:
            location_name = entry["locationName"]
            deployment_metadata = check.inst(
                deserialize_json_to_dagster_namedtuple(entry["serializedDeploymentMetadata"]),
                CodeDeploymentMetadata,
            )
            deployment_map[location_name] = deployment_metadata
            if entry["hasOutdatedData"]:
                force_update_locations.add(location_name)

        user_code_launcher.update_grpc_metadata(deployment_map, force_update_locations)

    def _get_grpc_client(
        self,
        user_code_launcher: DagsterCloudUserCodeLauncher,
        repository_location_origin: RepositoryLocationOrigin,
    ) -> DagsterGrpcClient:
        endpoint = user_code_launcher.get_grpc_endpoint(repository_location_origin)
        return endpoint.create_client()

    def _to_event_log_entries(
        self, context: DagsterCloudStepHandlerContext, events: List[DagsterEvent]
    ) -> List[EventLogEntry]:

        args = context.execute_step_args
        assert len(args.step_keys_to_execute) == 1
        step_key = args.step_keys_to_execute[0]

        return [
            EventLogEntry(
                message=event.message,
                user_message=event.message,
                level=logging.INFO,
                pipeline_name=context.pipeline_run.pipeline_name,
                run_id=context.pipeline_run.run_id,
                error_info=None,
                timestamp=time.time(),
                step_key=step_key,
                dagster_event=event,
            )
            for event in events
        ]

    def _handle_api_request(
        self,
        request: DagsterCloudApiRequest,
        instance: DagsterCloudAgentInstance,
        user_code_launcher: DagsterCloudUserCodeLauncher,
    ) -> Union[DagsterCloudApiSuccess, DagsterCloudApiGrpcResponse]:
        api_name = request.request_api
        if api_name == DagsterCloudApi.CHECK_FOR_WORKSPACE_UPDATES:
            self._check_for_workspace_updates(instance, user_code_launcher)
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
                f"Received request from {instance.dagster_cloud_url} to launch all steps for pipeline {run.pipeline_name}",
                run,
                cls=self.__class__,
            )

            launcher = user_code_launcher.run_launcher()
            launcher.launch_run(LaunchRunContext(pipeline_run=run, workspace=None))
            return DagsterCloudApiSuccess()
        elif api_name == DagsterCloudApi.CHECK_RUN_HEALTH:
            run = request.request_args.pipeline_run
            launcher = user_code_launcher.run_launcher()
            check.inst(launcher, WatchfulRunLauncher)
            launcher.check_run_health(run.run_id)
            return DagsterCloudApiSuccess()
        elif api_name == DagsterCloudApi.TERMINATE_RUN:
            run = request.request_args.pipeline_run
            instance.report_engine_event(
                f"Received request from {instance.dagster_cloud_url} to terminate run",
                run,
                cls=self.__class__,
            )

            launcher = user_code_launcher.run_launcher()
            launcher.terminate(run.run_id)
            return DagsterCloudApiSuccess()
        elif api_name == DagsterCloudApi.LAUNCH_STEP:
            context = DagsterCloudStepHandlerContext.deserialize(
                instance, request.request_args.persisted_step_handler_context
            )
            args = context.execute_step_args

            instance.report_engine_event(
                f"Received request from {instance.dagster_cloud_url} to launch steps: {', '.join(args.step_keys_to_execute)}",
                context.pipeline_run,
                cls=self.__class__,
            )

            execution_config = context.pipeline_run.run_config.get("execution")
            step_handler = user_code_launcher.get_step_handler(execution_config)
            events = step_handler.launch_step(context)
            event_records = self._to_event_log_entries(context, events)
            for event_record in event_records:
                instance.handle_new_event(event_record)

            return DagsterCloudApiSuccess()

        elif api_name == DagsterCloudApi.TERMINATE_STEP:
            context = DagsterCloudStepHandlerContext.deserialize(
                instance, request.request_args.persisted_step_handler_context
            )

            execution_config = context.pipeline_run.run_config.get("execution")
            step_handler = user_code_launcher.get_step_handler(execution_config)
            events = step_handler.terminate_step(context)
            event_records = self._to_event_log_entries(context, events)
            for event_record in event_records:
                instance.handle_new_event(event_record)

            return DagsterCloudApiSuccess()

        elif api_name == DagsterCloudApi.CHECK_STEP_HEALTH:
            context = DagsterCloudStepHandlerContext.deserialize(
                instance, request.request_args.persisted_step_handler_context
            )

            execution_config = context.pipeline_run.run_config.get("execution")
            step_handler = user_code_launcher.get_step_handler(execution_config)
            events = step_handler.check_step_health(context)
            event_records = self._to_event_log_entries(context, events)
            for event_record in event_records:
                instance.handle_new_event(event_record)

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

        if request_api not in DagsterCloudApi.__members__:
            api_result = DagsterCloudApiUnknownCommandResponse(request_api)
            self._logger.warning(
                "Ignoring request {request}: Unknown command. This is likely due to running an "
                "older version of the agent.".format(request=json_request)
            )
        else:
            try:
                request = deserialize_json_to_dagster_namedtuple(request_body)
                self._logger.info(
                    "Agent has received request {request}.".format(
                        request=request,
                    )
                )
                api_result = self._handle_api_request(request, instance, user_code_launcher)
            except Exception:  # pylint: disable=broad-except
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                api_result = DagsterCloudApiErrorResponse(error_infos=[error_info])
                self._logger.error(
                    "Error serving request {request}: {error_info}".format(
                        request=json_request,
                        error_info=error_info,
                    )
                )

        thread_finished_request_time = pendulum.now("UTC").timestamp()
        thread_telemetry = DagsterCloudApiThreadTelemetry(
            submitted_to_executor_timestamp=submitted_to_executor_timestamp,
            thread_start_run_timestamp=thread_start_run_timestamp,
            thread_end_handle_api_request_timestamp=thread_finished_request_time,
        )

        api_result = serialize_dagster_namedtuple(
            api_result.with_thread_telemetry(thread_telemetry)
        )

        instance.graphql_client.execute(
            SEND_USER_CLOUD_RESPONSE_MUTATION,
            {
                "requestId": request_id,
                "requestApi": request_api,
                "response": api_result,
            },
        )

        return error_info

    def run_iteration(
        self, instance: DagsterCloudAgentInstance, user_code_launcher: DagsterCloudUserCodeLauncher
    ) -> Iterator[Optional[SerializableErrorInfo]]:
        if not self._initial_workspace_loaded:
            self._check_for_workspace_updates(instance, user_code_launcher)
            self._initial_workspace_loaded = True

        result = instance.graphql_client.execute(GET_USER_CLOUD_REQUESTS_QUERY)
        json_requests = result["data"]["userCloudAgent"]["popUserCloudAgentRequests"]

        self._logger.debug(
            "Iteration #{iteration}: Agent adding {num_requests} requests to process.".format(
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
                except (concurrent.futures.CancelledError, concurrent.futures.TimeoutError):
                    response = serializable_error_info_from_exc_info(sys.exc_info())

                # Do not process a request again once we have its result
                del self._request_ids_to_future_context[request_id]

                # Yield the error information from the future
                if response:
                    yield response

        self._iteration += 1

        yield None
