from collections import namedtuple
from datetime import timedelta
from enum import Enum
from typing import Any, List, Mapping, NamedTuple, Optional, Sequence, Union

import dagster._check as check
import pendulum
from dagster.core.code_pointer import CodePointer
from dagster.core.host_representation import ExternalRepositoryData, RepositoryLocationOrigin
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.serdes import whitelist_for_serdes
from dagster.utils.error import SerializableErrorInfo
from dagster_cloud.execution.monitoring import CloudRunWorkerStatuses
from dagster_cloud.workspace.origin import CodeDeploymentMetadata

DEFAULT_EXPIRATION_MILLISECONDS = 10 * 60 * 1000


@whitelist_for_serdes
class DagsterCloudUploadRepositoryData(NamedTuple):
    """
    Serialized object uploaded by the Dagster Cloud agent with information pulled
    from a gRPC server about an individual repository - the data field is serialized since the
    agent may be running older code that doesn't know how to deserialize it, so it passes
    it serialized up to the host cloud, which is always up to date.
    """

    repository_name: str
    code_pointer: CodePointer
    serialized_repository_data: str


@whitelist_for_serdes
class DagsterCloudUploadLocationData(NamedTuple):
    """
    Serialized object uploaded by the Dagster Cloud agent with information pulled
    about a successfully loaded repository location, including information about
    each repository as well as shared metadata like the image to use when launching
    runs in this location.
    """

    upload_repository_datas: List[DagsterCloudUploadRepositoryData]
    container_image: Optional[str]
    executable_path: Optional[str]


@whitelist_for_serdes
class DagsterCloudUploadWorkspaceEntry(NamedTuple):
    """
    Serialized object uploaded by the Dagster Cloud agent with information about
    a repository location - either the serialized DagsterCloudUploadLocationData
    if the location loaded succesfully, or a SerializableErrorInfo describing the
    error if it was not.
    """

    location_name: str
    deployment_metadata: CodeDeploymentMetadata
    upload_location_data: Optional[DagsterCloudUploadLocationData]
    serialized_error_info: Optional[SerializableErrorInfo]


@whitelist_for_serdes
class DagsterCloudApi(Enum):
    CHECK_FOR_WORKSPACE_UPDATES = "CHECK_FOR_WORKSPACE_UPDATES"
    LOAD_REPOSITORIES = "LOAD_REPOSITORIES"
    GET_EXTERNAL_EXECUTION_PLAN = "GET_EXTERNAL_EXECUTION_PLAN"
    GET_SUBSET_EXTERNAL_PIPELINE_RESULT = "GET_SUBSET_EXTERNAL_PIPELINE_RESULT"
    GET_EXTERNAL_PARTITION_CONFIG = "GET_EXTERNAL_PARTITION_CONFIG"
    GET_EXTERNAL_PARTITION_TAGS = "GET_EXTERNAL_PARTITION_TAGS"
    GET_EXTERNAL_PARTITION_NAMES = "GET_EXTERNAL_PARTITION_NAMES"
    GET_EXTERNAL_PARTITION_SET_EXECUTION_PARAM_DATA = (
        "GET_EXTERNAL_PARTITION_SET_EXECUTION_PARAM_DATA"
    )
    GET_EXTERNAL_SCHEDULE_EXECUTION_DATA = "GET_EXTERNAL_SCHEDULE_EXECUTION_DATA"
    GET_EXTERNAL_SENSOR_EXECUTION_DATA = "GET_EXTERNAL_SENSOR_EXECUTION_DATA"
    GET_EXTERNAL_NOTEBOOK_DATA = "GET_EXTERNAL_NOTEBOOK_DATA"

    LAUNCH_RUN = "LAUNCH_RUN"
    CHECK_RUN_HEALTH = "CHECK_RUN_HEALTH"  # deprecated, agents now surface this in heartbeats
    TERMINATE_RUN = "TERMINATE_RUN"
    LAUNCH_STEP = "LAUNCH_STEP"  # deprecated with cloud executor
    CHECK_STEP_HEALTH = "CHECK_STEP_HEALTH"  # deprecated with cloud executor
    TERMINATE_STEP = "TERMINATE_STEP"  # deprecated with cloud executor

    def __structlog__(self):
        return self.name


@whitelist_for_serdes
class DagsterCloudApiThreadTelemetry(
    NamedTuple(
        "_DagsterCloudApiThreadTelemetry",
        [
            ("submitted_to_executor_timestamp", float),
            ("thread_start_run_timestamp", float),
            ("thread_end_handle_api_request_timestamp", float),
        ],
    )
):
    def __new__(
        cls,
        submitted_to_executor_timestamp: float,
        thread_start_run_timestamp: float,
        thread_end_handle_api_request_timestamp: float,
    ):
        return super(cls, DagsterCloudApiThreadTelemetry).__new__(
            cls,
            check.float_param(submitted_to_executor_timestamp, "submitted_to_executor_timestamp"),
            check.float_param(thread_start_run_timestamp, "thread_start_run_timestamp"),
            check.float_param(
                thread_end_handle_api_request_timestamp, "thread_end_handle_api_request_timestamp"
            ),
        )

    @property
    def time_to_thread_initialization_seconds(self) -> float:
        return self.thread_start_run_timestamp - self.submitted_to_executor_timestamp

    @property
    def time_to_handle_api_request_seconds(self) -> float:
        return self.thread_end_handle_api_request_timestamp - self.thread_start_run_timestamp


@whitelist_for_serdes
class DagsterCloudApiSuccess(
    NamedTuple(
        "_DagsterCloudApiSuccess", [("thread_telemetry", Optional[DagsterCloudApiThreadTelemetry])]
    )
):
    def __new__(cls, thread_telemetry: Optional[DagsterCloudApiThreadTelemetry] = None):
        return super(cls, DagsterCloudApiSuccess).__new__(
            cls,
            check.opt_inst_param(
                thread_telemetry, "thread_telemetry", DagsterCloudApiThreadTelemetry
            ),
        )

    def with_thread_telemetry(self, thread_telemetry: DagsterCloudApiThreadTelemetry):
        return self._replace(thread_telemetry=thread_telemetry)


@whitelist_for_serdes
class DagsterCloudApiUnknownCommandResponse(
    NamedTuple(
        "_DagsterCloudApiUnknownCommandResponse",
        [("request_api", str), ("thread_telemetry", Optional[DagsterCloudApiThreadTelemetry])],
    )
):
    def __new__(
        cls, request_api: str, thread_telemetry: Optional[DagsterCloudApiThreadTelemetry] = None
    ):
        return super(cls, DagsterCloudApiUnknownCommandResponse).__new__(
            cls,
            check.str_param(request_api, "request_api"),
            check.opt_inst_param(
                thread_telemetry, "thread_telemetry", DagsterCloudApiThreadTelemetry
            ),
        )

    def with_thread_telemetry(self, thread_telemetry: DagsterCloudApiThreadTelemetry):
        return self._replace(thread_telemetry=thread_telemetry)


@whitelist_for_serdes
class DagsterCloudApiErrorResponse(
    NamedTuple(
        "_DagsterCloudApiErrorResponse",
        [
            ("error_infos", List[SerializableErrorInfo]),
            ("thread_telemetry", Optional[DagsterCloudApiThreadTelemetry]),
        ],
    )
):
    def __new__(
        cls,
        error_infos: List[SerializableErrorInfo],
        thread_telemetry: Optional[DagsterCloudApiThreadTelemetry] = None,
    ):
        return super(cls, DagsterCloudApiErrorResponse).__new__(
            cls,
            check.list_param(error_infos, "error_infos", of_type=SerializableErrorInfo),
            check.opt_inst_param(
                thread_telemetry, "thread_telemetry", DagsterCloudApiThreadTelemetry
            ),
        )

    def with_thread_telemetry(self, thread_telemetry: DagsterCloudApiThreadTelemetry):
        return self._replace(thread_telemetry=thread_telemetry)


@whitelist_for_serdes
class DagsterCloudApiGrpcResponse(
    NamedTuple(
        "_DagsterCloudApiGrpcResponse",
        [
            ("serialized_response_or_error", str),
            ("thread_telemetry", Optional[DagsterCloudApiThreadTelemetry]),
        ],
    )
):
    # Class that DagsterCloudApi methods can use to pass along the result of
    # a gRPC call against the user code server. The field here is passed in
    # serialized as a string, because we can't guarantee that the agent code will
    # be up-to-date enough to know how to deserialize it (but the host cloud always
    # should, since it will always be up to date).
    def __new__(
        cls,
        serialized_response_or_error: str,
        thread_telemetry: Optional[DagsterCloudApiThreadTelemetry] = None,
    ):
        return super(cls, DagsterCloudApiGrpcResponse).__new__(
            cls,
            check.str_param(serialized_response_or_error, "serialized_response_or_error"),
            check.opt_inst_param(
                thread_telemetry, "thread_telemetry", DagsterCloudApiThreadTelemetry
            ),
        )

    def with_thread_telemetry(self, thread_telemetry: DagsterCloudApiThreadTelemetry):
        return self._replace(thread_telemetry=thread_telemetry)


@whitelist_for_serdes
class LoadRepositoriesArgs(namedtuple("_LoadRepositoryArgs", "location_origin")):
    def __new__(cls, location_origin):
        return super(cls, LoadRepositoriesArgs).__new__(
            cls,
            check.inst_param(location_origin, "location_origin", RepositoryLocationOrigin),
        )


@whitelist_for_serdes
class DagsterCloudRepositoryData(
    namedtuple("_DagsterCloudRepositoryData", "repo_name code_pointer external_repository_data")
):
    def __new__(cls, repo_name, code_pointer, external_repository_data):
        return super(cls, DagsterCloudRepositoryData).__new__(
            cls,
            check.str_param(repo_name, "repo_name"),
            check.inst_param(code_pointer, "code_pointer", CodePointer),
            check.inst_param(
                external_repository_data,
                "external_repository_data",
                ExternalRepositoryData,
            ),
        )


@whitelist_for_serdes
class LoadRepositoriesResponse(
    namedtuple(
        "_LoadRepositoriesResponse",
        "repository_datas container_image executable_path code_deployment_metadata",
    )
):
    def __new__(
        cls, repository_datas, container_image, executable_path, code_deployment_metadata=None
    ):
        return super(cls, LoadRepositoriesResponse).__new__(
            cls,
            check.list_param(
                repository_datas,
                "repository_datas",
                of_type=DagsterCloudRepositoryData,
            ),
            check.opt_str_param(container_image, "container_image"),
            check.opt_str_param(executable_path, "executable_path"),
            check.opt_inst_param(
                code_deployment_metadata, "code_deployment_metadata", CodeDeploymentMetadata
            ),
        )


@whitelist_for_serdes
class LaunchRunArgs(namedtuple("_LaunchRunArgs", "pipeline_run")):
    def __new__(cls, pipeline_run):
        return super(cls, LaunchRunArgs).__new__(
            cls,
            check.inst_param(pipeline_run, "pipeline_run", PipelineRun),
        )


@whitelist_for_serdes
class TerminateRunArgs(namedtuple("_TerminateRunArgs", "pipeline_run")):
    def __new__(cls, pipeline_run):
        return super(cls, TerminateRunArgs).__new__(
            cls,
            check.inst_param(pipeline_run, "pipeline_run", PipelineRun),
        )


@whitelist_for_serdes
class DagsterCloudApiRequest(
    NamedTuple(
        "_DagsterCloudApiRequest",
        [
            ("request_id", str),
            ("request_api", DagsterCloudApi),
            ("request_args", Any),
            ("expire_at", float),
        ],
    )
):
    def __new__(
        cls,
        request_id: str,
        request_api: DagsterCloudApi,
        request_args: Any,
        expire_at: Optional[float] = None,
    ):
        return super(cls, DagsterCloudApiRequest).__new__(
            cls,
            check.str_param(request_id, "request_id"),
            check.inst_param(request_api, "request_api", DagsterCloudApi),
            request_args,
            check.opt_float_param(
                expire_at,
                "expire_at",
                default=(
                    pendulum.now("UTC") + timedelta(milliseconds=DEFAULT_EXPIRATION_MILLISECONDS)
                ).timestamp(),
            ),
        )

    @property
    def is_expired(self) -> bool:
        return pendulum.now("UTC").timestamp() > self.expire_at

    @staticmethod
    def format_request(request_id: str, request_api: Union[str, DagsterCloudApi]) -> str:
        return f"[{request_id}: {request_api}]"

    def __str__(self) -> str:
        return DagsterCloudApiRequest.format_request(self.request_id, self.request_api)


DagsterCloudApiResponse = Union[
    DagsterCloudApiSuccess,
    DagsterCloudApiGrpcResponse,
    DagsterCloudApiErrorResponse,
    DagsterCloudApiUnknownCommandResponse,
]


@whitelist_for_serdes
class DagsterCloudUploadApiResponse(
    NamedTuple(
        "_DagsterCloudUploadApiResponse",
        [
            ("request_id", str),
            ("request_api", DagsterCloudApi),
            ("response", DagsterCloudApiResponse),
        ],
    )
):
    pass


@whitelist_for_serdes
class TimestampedError(namedtuple("_TimestampedError", "timestamp error")):
    def __new__(cls, timestamp, error):

        return super(TimestampedError, cls).__new__(
            cls,
            timestamp=check.opt_float_param(timestamp, "timestamp"),
            error=check.inst_param(error, "error", SerializableErrorInfo),
        )


@whitelist_for_serdes
class AgentHeartbeat(
    NamedTuple(
        "_AgentHeartbeat",
        [
            ("timestamp", float),
            ("agent_id", str),
            ("agent_label", Optional[str]),
            ("agent_type", Optional[str]),
            ("errors", Optional[Sequence[TimestampedError]]),
            ("metadata", Optional[Mapping[str, str]]),
            ("run_worker_statuses", Optional[CloudRunWorkerStatuses]),
        ],
    )
):
    def __new__(
        cls,
        timestamp: float,
        agent_id: str,
        agent_label: Optional[str],
        agent_type: Optional[str],
        errors: Optional[Sequence[TimestampedError]] = None,
        metadata: Optional[Mapping[str, str]] = None,
        run_worker_statuses: Optional[CloudRunWorkerStatuses] = None,
    ):
        return super(AgentHeartbeat, cls).__new__(
            cls,
            timestamp=check.float_param(timestamp, "timestamp"),
            agent_id=check.str_param(agent_id, "agent_id"),
            agent_label=check.opt_str_param(agent_label, "agent_label"),
            agent_type=check.opt_str_param(agent_type, "agent_type"),
            errors=check.opt_list_param(errors, "errors", of_type=TimestampedError),
            metadata=check.opt_dict_param(metadata, "metadata", str),
            run_worker_statuses=check.opt_inst_param(
                run_worker_statuses, "run_worker_statuses", CloudRunWorkerStatuses
            ),
        )


@whitelist_for_serdes
class DagsterCloudSandboxConnectionInfo(
    NamedTuple(
        "_DagsterCloudSandboxConnectionInfo",
        [
            ("username", str),
            ("hostname", str),
            ("port", int),
        ],
    )
):
    def __new__(cls, username: str, hostname: str, port: int):
        return super(DagsterCloudSandboxConnectionInfo, cls).__new__(
            cls,
            check.str_param(username, "username"),
            check.str_param(hostname, "hostname"),
            check.int_param(port, "port"),
        )


@whitelist_for_serdes
class DagsterCloudSandboxProxyInfo(
    NamedTuple(
        "_DagsterCloudSandboxProxyInfo",
        [
            ("hostname", str),
            ("port", int),
            ("auth_token", str),
            ("min_port", int),
            ("max_port", int),
        ],
    )
):
    def __new__(cls, hostname: str, port: int, auth_token: str, min_port: int, max_port: int):
        return super(DagsterCloudSandboxProxyInfo, cls).__new__(
            cls,
            check.str_param(hostname, "hostname"),
            check.int_param(port, "port"),
            check.str_param(auth_token, "auth_token"),
            check.int_param(min_port, "min_port"),
            check.int_param(max_port, "max_port"),
        )
