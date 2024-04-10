# The following allows logging calls with extra arguments
# ruff: noqa: PLE1205
import json
import logging
import os
import sys
import tempfile
import threading
import time
import zlib
from abc import abstractmethod, abstractproperty
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from contextlib import AbstractContextManager
from typing import (
    Any,
    Callable,
    Collection,
    DefaultDict,
    Dict,
    Generic,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import dagster._check as check
import grpc
import pendulum
from dagster import BoolSource, Field, IntSource
from dagster._api.list_repositories import sync_list_repositories_grpc
from dagster._core.definitions.selector import JobSelector
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.instance import MayHaveInstanceWeakref
from dagster._core.launcher import RunLauncher
from dagster._core.remote_representation import RemoteRepositoryOrigin
from dagster._core.remote_representation.origin import (
    CodeLocationOrigin,
    RegisteredCodeLocationOrigin,
)
from dagster._grpc.client import DagsterGrpcClient
from dagster._grpc.types import GetCurrentImageResult
from dagster._serdes import deserialize_value, serialize_value, whitelist_for_serdes
from dagster._utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster._utils.merger import merge_dicts
from dagster._utils.typed_dict import init_optional_typeddict
from dagster_cloud_cli.core.errors import raise_http_error
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata
from typing_extensions import Self, TypeAlias

from dagster_cloud.agent.queries import GET_AGENTS_QUERY
from dagster_cloud.api.dagster_cloud_api import (
    DagsterCloudUploadLocationData,
    DagsterCloudUploadRepositoryData,
    DagsterCloudUploadWorkspaceEntry,
    DagsterCloudUploadWorkspaceResponse,
    UserCodeDeploymentType,
)
from dagster_cloud.execution.monitoring import (
    CloudCodeServerHeartbeat,
    CloudCodeServerHeartbeatMetadata,
    CloudCodeServerStatus,
    CloudCodeServerUtilizationMetrics,
    CloudContainerResourceLimits,
    CloudRunWorkerStatus,
    CloudRunWorkerStatuses,
    start_run_worker_monitoring_thread,
)
from dagster_cloud.instance import DagsterCloudAgentInstance
from dagster_cloud.pex.grpc.client import MultiPexGrpcClient
from dagster_cloud.pex.grpc.types import (
    CreatePexServerArgs,
    GetPexServersArgs,
    PexServerHandle,
    ShutdownPexServerArgs,
)
from dagster_cloud.util import diff_serializable_namedtuple_map

DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT = 180
DEFAULT_MAX_TTL_SERVERS = 25
ACTIVE_AGENT_HEARTBEAT_INTERVAL = int(
    os.getenv("DAGSTER_CLOUD_ACTIVE_AGENT_HEARTBEAT_INVERAL", "600")
)


USER_CODE_LAUNCHER_RECONCILE_SLEEP_SECONDS = 1
USER_CODE_LAUNCHER_RECONCILE_METRICS_SLEEP_SECONDS = 60

# Check on pending delete servers every 30th reconcile
PENDING_DELETE_SERVER_CHECK_INTERVAL = 30

# How often to sync actual_entries with server liveness
ACTUAL_ENTRIES_REFRESH_INTERVAL = 30

CLEANUP_SERVER_GRACE_PERIOD_SECONDS = 3600

ServerHandle = TypeVar("ServerHandle")

DEPLOYMENT_INFO_QUERY = """
    query DeploymentInfo {
         deploymentInfo {
             deploymentType
         }
     }
"""

INIT_UPLOAD_LOCATIONS_QUERY = """
    query WorkspaceEntries {
        workspace {
            workspaceEntries {
                locationName
            }
        }
    }
"""

DEFAULT_SERVER_TTL_SECONDS = 60 * 60 * 24


@whitelist_for_serdes
class UserCodeLauncherEntry(
    NamedTuple(
        "_UserCodeLauncherEntry",
        [
            ("code_deployment_metadata", CodeDeploymentMetadata),
            ("update_timestamp", float),
        ],
    )
):
    def __new__(
        cls,
        code_deployment_metadata,
        update_timestamp,
    ):
        return super(UserCodeLauncherEntry, cls).__new__(
            cls,
            check.inst_param(
                code_deployment_metadata, "code_deployment_metadata", CodeDeploymentMetadata
            ),
            check.float_param(update_timestamp, "update_timestamp"),
        )


SHARED_USER_CODE_LAUNCHER_CONFIG = {
    "server_ttl": Field(
        {
            "full_deployments": Field(
                {
                    "enabled": Field(
                        BoolSource,
                        is_required=True,
                        description=(
                            "Whether to shut down servers created by the agent for full deployments"
                            " when they are not serving requests"
                        ),
                    ),
                    "ttl_seconds": Field(
                        IntSource,
                        is_required=False,
                        default_value=DEFAULT_SERVER_TTL_SECONDS,
                        description=(
                            "If the `enabled` flag is set , how long to leave a server running for"
                            " a once it has been launched. Decreasing this value will cause fewer"
                            " servers to be running at once, but request latency may increase if"
                            " more requests need to wait for a server to launch"
                        ),
                    ),
                },
                is_required=False,
            ),
            "branch_deployments": Field(
                {
                    # No enabled flag, because branch deployments always have a TTL
                    "ttl_seconds": Field(
                        IntSource,
                        is_required=False,
                        default_value=DEFAULT_SERVER_TTL_SECONDS,
                        description=(
                            "How long to leave a server for a branch deployment running once it has"
                            " been launched. Decreasing this value will cause fewer servers to be"
                            " running at once, but request latency may increase if more requests"
                            " need to wait for a server to launch"
                        ),
                    ),
                },
                is_required=False,
            ),
            "max_servers": Field(
                IntSource,
                is_required=False,
                default_value=DEFAULT_MAX_TTL_SERVERS,
                description=(
                    "In addition to the TTL, ensure that the maximum number of servers that are up"
                    " at any given time and not currently serving requests stays below this number."
                ),
            ),
            "enabled": Field(
                BoolSource,
                is_required=False,
                description="Deprecated - use `full_deployments.enabled` instead",
            ),
            "ttl_seconds": Field(
                IntSource,
                is_required=False,
                description="Deprecated - use `full_deployments.ttl_seconds` instead",
            ),
        },
        is_required=False,
    ),
    "defer_job_snapshots": Field(
        BoolSource,
        is_required=False,
        default_value=True,
        description=(
            "Do not include full job snapshots in the workspace "
            "snapshot, upload them separately if they have not been previously uploaded."
        ),
    ),
    "upload_snapshots_on_startup": Field(
        BoolSource,
        is_required=False,
        default_value=True,
        description=(
            "Upload information about code locations to Dagster Cloud whenever the "
            "agent starts up, even if the code location has not changed since the last upload."
        ),
    ),
    "requires_healthcheck": Field(
        BoolSource,
        is_required=False,
        default_value=False,
        description=(
            "Whether the agent update process expects a liveness sentinel to be written before an"
            " agent is considered healthy. If using zero-downtime agent updates, this should be set"
            " to True."
        ),
    ),
}

DeploymentAndLocation: TypeAlias = Tuple[str, str]
UserCodeLauncherEntryMap: TypeAlias = Dict[DeploymentAndLocation, UserCodeLauncherEntry]


class ServerEndpoint(
    NamedTuple(
        "_ServerEndpoint",
        [
            ("host", str),
            ("port", Optional[int]),
            ("socket", Optional[str]),
            ("metadata", Optional[List[Tuple[str, str]]]),
        ],
    )
):
    def __new__(cls, host, port, socket, metadata=None):
        return super(ServerEndpoint, cls).__new__(
            cls,
            check.str_param(host, "host"),
            check.opt_int_param(port, "port"),
            check.opt_str_param(socket, "socket"),
            check.opt_list_param(metadata, "metadata"),
        )

    def create_client(self) -> DagsterGrpcClient:
        return DagsterGrpcClient(
            port=self.port, socket=self.socket, host=self.host, metadata=self.metadata
        )

    def create_multipex_client(self) -> MultiPexGrpcClient:
        return MultiPexGrpcClient(port=self.port, socket=self.socket, host=self.host)

    def with_metadata(self, metadata: Optional[List[Tuple[str, str]]]):
        return self._replace(metadata=metadata)


class DagsterCloudGrpcServer(
    NamedTuple(
        "_DagsterCloudGrpcServer",
        [
            ("server_handle", Any),  # No Generic NamedTuples yet sadly
            ("server_endpoint", ServerEndpoint),
            ("code_deployment_metadata", CodeDeploymentMetadata),
        ],
    ),
):
    def __new__(
        cls,
        server_handle: Any,
        server_endpoint: ServerEndpoint,
        code_deployment_metadata: CodeDeploymentMetadata,
    ):
        return super(DagsterCloudGrpcServer, cls).__new__(
            cls,
            server_handle,
            check.inst_param(server_endpoint, "server_endpoint", ServerEndpoint),
            check.inst_param(
                code_deployment_metadata, "code_deployment_metadata", CodeDeploymentMetadata
            ),
        )


class DagsterCloudUserCodeLauncher(
    AbstractContextManager, MayHaveInstanceWeakref[DagsterCloudAgentInstance], Generic[ServerHandle]
):
    def __init__(
        self,
        server_ttl: Optional[dict] = None,
        defer_job_snapshots: bool = True,
        server_process_startup_timeout=None,
        upload_snapshots_on_startup: bool = True,
        requires_healthcheck: bool = False,
        code_server_metrics: Optional[Mapping[str, Any]] = None,
        agent_metrics: Optional[Mapping[str, Any]] = None,
    ):
        self._grpc_servers: Dict[
            DeploymentAndLocation, Union[DagsterCloudGrpcServer, SerializableErrorInfo]
        ] = {}
        self._first_unavailable_times: Dict[DeploymentAndLocation, float] = {}

        self._pending_delete_grpc_server_handles: Set[ServerHandle] = set()
        self._grpc_servers_lock = threading.Lock()
        self._per_location_metrics: Dict[
            DeploymentAndLocation, CloudCodeServerUtilizationMetrics
        ] = DefaultDict(lambda: init_optional_typeddict(CloudCodeServerUtilizationMetrics))

        self._multipex_servers: Dict[DeploymentAndLocation, DagsterCloudGrpcServer] = {}

        self._server_ttl_config = check.opt_dict_param(server_ttl, "server_ttl")
        self._defer_job_snapshots = defer_job_snapshots
        self.upload_snapshots_on_startup = check.bool_param(
            upload_snapshots_on_startup, "upload_snapshots_on_startup"
        )
        self._requires_healthcheck = check.bool_param(requires_healthcheck, "requires_healthcheck")

        # periodically reconciles to make desired = actual
        self._desired_entries: Dict[DeploymentAndLocation, UserCodeLauncherEntry] = {}
        self._actual_entries: Dict[DeploymentAndLocation, UserCodeLauncherEntry] = {}
        self._last_refreshed_actual_entries = 0
        self._last_cleaned_up_dangling_code_servers = 0
        self._metadata_lock = threading.Lock()

        self._upload_locations: Set[DeploymentAndLocation] = set()

        self._logger = logging.getLogger("dagster_cloud.user_code_launcher")
        self._event_logger = logging.getLogger("cloud-events")
        self._started: bool = False
        self._run_worker_monitoring_thread = None
        self._run_worker_monitoring_thread_shutdown_event = None
        self._run_worker_deployments_to_check: Set[str] = set()
        self._run_worker_statuses_dict: Dict[str, List[CloudRunWorkerStatus]] = {}
        self._run_worker_monitoring_lock = threading.Lock()

        self._reconcile_count = 0
        self._reconcile_grpc_metadata_shutdown_event = threading.Event()
        self._reconcile_grpc_metadata_thread = None

        self._reconcile_location_utilization_metrics_shutdown_event = threading.Event()
        self._reconcile_location_utilization_metrics_thread = None

        self._server_process_startup_timeout = check.opt_int_param(
            server_process_startup_timeout,
            "server_process_startup_timeout",
            DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
        )

        self._code_server_metrics_config = code_server_metrics
        self._agent_metrics_config = agent_metrics
        super().__init__()

    def get_active_grpc_server_handles(self) -> List[ServerHandle]:
        with self._grpc_servers_lock:
            return [
                s.server_handle
                for s in self._grpc_servers.values()
                if not isinstance(s, SerializableErrorInfo)
            ] + list(self._pending_delete_grpc_server_handles)

    def get_active_agent_ids(self) -> Set[str]:
        try:
            result = self._instance.organization_scoped_graphql_client().execute(
                GET_AGENTS_QUERY,
                {"heartbeatedSince": time.time() - ACTIVE_AGENT_HEARTBEAT_INTERVAL},
            )
        except:
            self._logger.exception(
                "Could not connect to graphql server to "
                "retrieve active agent_ids. Just returning this agent as an active ID."
            )
            return set([self._instance.instance_uuid])
        self._logger.info(f"Active agent ids response: {result}")
        return set(agent_data["id"] for agent_data in result["data"]["agents"])

    @property
    def code_server_metrics_enabled(self) -> bool:
        return (
            self._code_server_metrics_config["enabled"]
            if self._code_server_metrics_config
            else False
        )

    @property
    def agent_metrics_enabled(self) -> bool:
        return self._agent_metrics_config["enabled"] if self._agent_metrics_config else False

    @property
    def server_ttl_enabled_for_full_deployments(self) -> bool:
        if "enabled" in self._server_ttl_config:
            return self._server_ttl_config["enabled"]

        if "full_deployments" not in self._server_ttl_config:
            return False

        return self._server_ttl_config["full_deployments"].get("enabled", True)

    @property
    def full_deployment_ttl_seconds(self) -> int:
        if "ttl_seconds" in self._server_ttl_config:
            return self._server_ttl_config["ttl_seconds"]

        return self._server_ttl_config.get("full_deployments", {}).get(
            "ttl_seconds", DEFAULT_SERVER_TTL_SECONDS
        )

    @property
    def branch_deployment_ttl_seconds(self) -> int:
        return self._server_ttl_config.get("branch_deployments", {}).get(
            "ttl_seconds", DEFAULT_SERVER_TTL_SECONDS
        )

    @abstractproperty
    def user_code_deployment_type(self) -> UserCodeDeploymentType:
        raise NotImplementedError()

    @property
    def server_ttl_max_servers(self) -> int:
        return self._server_ttl_config.get("max_servers", DEFAULT_MAX_TTL_SERVERS)

    def start(self, run_reconcile_thread=True, run_metrics_thread=True):
        # Initialize
        check.invariant(
            not self._started,
            "Called start() on a DagsterCloudUserCodeLauncher that was already started",
        )
        # Begin spinning user code up and down
        self._started = True

        if self._instance.user_code_launcher.run_launcher().supports_check_run_worker_health and (
            self._instance.deployment_names or self._instance.include_all_serverless_deployments
        ):
            self._logger.debug("Starting run worker monitoring.")
            self._start_run_worker_monitoring()
        else:
            self._logger.debug(
                "Not starting run worker monitoring, because it's not supported on this agent."
            )

        self._graceful_cleanup_servers(
            include_own_servers=True  # shouldn't be any of our own servers at this part, but won't hurt either
        )

        if run_reconcile_thread:
            self._reconcile_grpc_metadata_thread = threading.Thread(
                target=self._reconcile_thread,
                args=(self._reconcile_grpc_metadata_shutdown_event,),
                name="grpc-reconcile-watch",
                daemon=True,
            )
            self._reconcile_grpc_metadata_thread.start()

        if run_metrics_thread and self.code_server_metrics_enabled:
            self._logger.info("Starting metrics reconciliation thread")
            self._reconcile_location_utilization_metrics_thread = threading.Thread(
                target=self._update_metrics_thread,
                args=(self._reconcile_location_utilization_metrics_shutdown_event,),
                name="location-utilization-metrics",
                daemon=True,
            )
            self._reconcile_location_utilization_metrics_thread.start()
        else:
            self._logger.info("Metrics not enabled: not starting metrics reconciliation thread.")

    def _start_run_worker_monitoring(self):
        # Utility method to be overridden by serverless subclass to change the monitoring interval
        (
            self._run_worker_monitoring_thread,
            self._run_worker_monitoring_thread_shutdown_event,
        ) = start_run_worker_monitoring_thread(
            self._instance,
            self._run_worker_deployments_to_check,
            self._run_worker_statuses_dict,
            self._run_worker_monitoring_lock,
        )

    def is_run_worker_monitoring_thread_alive(self):
        return (
            self._run_worker_monitoring_thread is not None
            and self._run_worker_monitoring_thread.is_alive()
        )

    def update_run_worker_monitoring_deployments(self, deployment_names):
        with self._run_worker_monitoring_lock:
            self._run_worker_deployments_to_check.clear()
            self._run_worker_deployments_to_check.update(deployment_names)

    def get_cloud_run_worker_statuses(self, deployment_names):
        supports_check = self._instance.run_launcher.supports_check_run_worker_health

        if not supports_check:
            return {
                deployment_name: CloudRunWorkerStatuses(
                    [],
                    run_worker_monitoring_supported=False,
                    run_worker_monitoring_thread_alive=None,
                )
                for deployment_name in deployment_names
            }

        self._logger.debug("Getting cloud run worker statuses for a heartbeat")

        with self._run_worker_monitoring_lock:
            # values are immutable, don't need deepcopy
            statuses_dict = self._run_worker_statuses_dict.copy()
        self._logger.debug(f"Returning statuses_dict: {statuses_dict}")

        is_alive = self.is_run_worker_monitoring_thread_alive()

        return {
            deployment_name: CloudRunWorkerStatuses(
                statuses=statuses_dict.get(deployment_name, []),
                run_worker_monitoring_supported=True,
                run_worker_monitoring_thread_alive=is_alive,
            )
            for deployment_name in deployment_names
        }

    def supports_origin(self, code_location_origin: CodeLocationOrigin) -> bool:
        return isinstance(code_location_origin, RegisteredCodeLocationOrigin)

    @property
    def supports_reload(self) -> bool:
        return False

    def _update_workspace_entry(
        self,
        deployment_name: str,
        workspace_entry: DagsterCloudUploadWorkspaceEntry,
        server_or_error: Union[DagsterCloudGrpcServer, SerializableErrorInfo],
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            dst = os.path.join(temp_dir, "workspace_entry.tmp")
            with open(dst, "wb") as f:
                f.write(zlib.compress(serialize_value(workspace_entry).encode("utf-8")))

            with open(dst, "rb") as f:
                self._logger.info(
                    f"Uploading workspace entry for {deployment_name}:{workspace_entry.location_name} ({os.path.getsize(dst)} bytes)"
                )

                resp = self._instance.requests_managed_retries_session.put(
                    self._instance.dagster_cloud_upload_workspace_entry_url,
                    headers=self._instance.headers_for_deployment(deployment_name),
                    data={},
                    files={"workspace_entry.tmp": f},
                    timeout=self._instance.dagster_cloud_api_timeout,
                    proxies=self._instance.dagster_cloud_api_proxies,
                )
                raise_http_error(resp)

            response = deserialize_value(resp.text, DagsterCloudUploadWorkspaceResponse)
            self._logger.info(
                "Workspace entry for"
                f" {deployment_name}:{workspace_entry.location_name} {response.message}"
            )

            # if the update took we are all done
            if response.updated or isinstance(server_or_error, SerializableErrorInfo):
                return

            # if not there must be missing job snapshots, upload them and then try again
            missing = response.missing_job_snapshots
            if missing is None:
                check.failed(
                    "Unexpected state: workspace was not updated but no required job snapshots were"
                    " returned."
                )

            self._logger.info(f"Uploading {len(missing)} job snapshots.")
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(
                        self.upload_job_snapshot, deployment_name, job_selector, server_or_error
                    )
                    for job_selector in missing
                ]
                wait(futures)
                # trigger any exceptions to throw
                _ = [f.result() for f in futures]

            with open(dst, "rb") as f:
                resp = self._instance.requests_managed_retries_session.put(
                    self._instance.dagster_cloud_upload_workspace_entry_url,
                    headers=self._instance.headers_for_deployment(deployment_name),
                    data={},
                    files={"workspace_entry.tmp": f},
                    timeout=self._instance.dagster_cloud_api_timeout,
                    proxies=self._instance.dagster_cloud_api_proxies,
                )
                raise_http_error(resp)

            response = deserialize_value(resp.text, DagsterCloudUploadWorkspaceResponse)

            if not response.updated:
                # this condition is expected to be extremely unlikely
                raise Exception(
                    "Upload failed, job definitions changed while uploading:"
                    f" {response.missing_job_snapshots}"
                )

            self._logger.info(
                "Workspace entry for"
                f" {deployment_name}:{workspace_entry.location_name} {response.message}"
            )

    def _get_upload_location_data(
        self,
        deployment_name: str,
        location_name: str,
        server: DagsterCloudGrpcServer,
    ) -> DagsterCloudUploadLocationData:
        location_origin = self._get_code_location_origin(location_name)
        client = server.server_endpoint.create_client()

        list_repositories_response = sync_list_repositories_grpc(client)

        upload_repo_datas: List[DagsterCloudUploadRepositoryData] = []

        for (
            repository_name,
            code_pointer,
        ) in list_repositories_response.repository_code_pointer_dict.items():
            external_repository_chunks = list(
                client.streaming_external_repository(
                    external_repository_origin=RemoteRepositoryOrigin(
                        location_origin,
                        repository_name,
                    ),
                    defer_snapshots=self._defer_job_snapshots,
                )
            )

            serialized_repository_data = "".join(
                [
                    chunk["serialized_external_repository_chunk"]
                    for chunk in external_repository_chunks
                ]
            )

            # Don't deserialize in case there are breaking changes - let the server do it
            upload_repo_datas.append(
                DagsterCloudUploadRepositoryData(
                    repository_name=repository_name,
                    code_pointer=code_pointer,
                    serialized_repository_data=serialized_repository_data,
                )
            )

        return DagsterCloudUploadLocationData(
            upload_repository_datas=upload_repo_datas,
            container_image=list_repositories_response.container_image
            # fallback to grpc call for versions that do not include it in response
            or deserialize_value(client.get_current_image(), GetCurrentImageResult).current_image,
            executable_path=list_repositories_response.executable_path,
            dagster_library_versions=list_repositories_response.dagster_library_versions,
        )

    def _update_location_error(
        self,
        deployment_name: str,
        location_name: str,
        error_info: SerializableErrorInfo,
        metadata: CodeDeploymentMetadata,
    ):
        self._logger.error(
            f"Unable to update {deployment_name}:{location_name}. Updating location with error data:"
            f" {error_info!s}."
        )

        # Update serialized error
        errored_workspace_entry = DagsterCloudUploadWorkspaceEntry(
            location_name=location_name,
            deployment_metadata=metadata,
            upload_location_data=None,
            serialized_error_info=error_info,
        )

        self._update_workspace_entry(
            deployment_name, errored_workspace_entry, server_or_error=error_info
        )

    def _update_location_data(
        self,
        deployment_name: str,
        location_name: str,
        server_or_error: Union[DagsterCloudGrpcServer, SerializableErrorInfo],
        metadata: CodeDeploymentMetadata,
    ) -> None:
        """Attempt to update Dagster Cloud with snapshots for this code location. If there's a failure
        writing (e.g. a timeout while generating the needed snapshots), will attempt to upload the
        error state to Dagster Cloud instead, then raise an Exception that must be caught and handled
        in the reconciliation loop in the callsite.
        """
        self._logger.info(f"Fetching metadata for {deployment_name}:{location_name}")

        if isinstance(server_or_error, SerializableErrorInfo):
            self._update_location_error(
                deployment_name,
                location_name,
                error_info=server_or_error,
                metadata=metadata,
            )
            return

        try:
            loaded_workspace_entry = DagsterCloudUploadWorkspaceEntry(
                location_name=location_name,
                deployment_metadata=metadata,
                upload_location_data=self._get_upload_location_data(
                    deployment_name,
                    location_name,
                    server_or_error,
                ),
                serialized_error_info=None,
            )

            self._logger.info(
                f"Updating {deployment_name}:{location_name} with repository load data"
            )

            self._update_workspace_entry(deployment_name, loaded_workspace_entry, server_or_error)
        except Exception:
            # Try to write the error to cloud.
            self._update_location_error(
                deployment_name,
                location_name,
                error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                metadata=metadata,
            )
            raise

    @property
    @abstractmethod
    def requires_images(self) -> bool:
        pass

    def _resolve_image(self, metadata: CodeDeploymentMetadata) -> Optional[str]:
        return metadata.image

    def _get_existing_pex_servers(
        self, deployment_name: str, location_name: str
    ) -> List[PexServerHandle]:
        server = self._multipex_servers.get((deployment_name, location_name))

        if not server:
            return []

        _server_handle, server_endpoint, _code_deployment_metadata = server
        try:
            return (
                server_endpoint.create_multipex_client()
                .get_pex_servers(
                    GetPexServersArgs(
                        deployment_name=deployment_name,
                        location_name=location_name,
                    )
                )
                .server_handles
            )
        except:
            error_info = serializable_error_info_from_exc_info(sys.exc_info())

            self._logger.error(
                "Error while fetching existing PEX servers from multipex server for"
                f" {deployment_name}:{location_name}: {error_info}"
            )
            return []

    @abstractmethod
    def _get_standalone_dagster_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[ServerHandle]:
        """Return a list of 'handles' that represent all running servers for a given location
        that are running the dagster grpc server as the entry point (i.e. are not multipex
        servers). Typically this will be a single server (unless an error was previous raised
        during a reconciliation loop. ServerHandle can be any type that is sufficient to uniquely
        identify the server and can be passed into _remove_server_handle to remove the server.
        """

    def _get_multipex_server_handles_for_location(
        self, _deployment_name: str, _location_name: str
    ) -> Collection[ServerHandle]:
        """Return a list of 'handles' that represent all servers running the multipex server
        entrypoint.
        """
        return []

    @abstractmethod
    def _start_new_server_spinup(
        self,
        deployment_name: str,
        location_name: str,
        desired_entry: UserCodeLauncherEntry,
    ) -> DagsterCloudGrpcServer:
        """Create a new server for the given location using the given metadata as configuration
        and return a ServerHandle indicating where it can be found. Any waiting for the server
        to happen should happen in _wait_for_new_server_ready.
        """

    def _wait_for_new_multipex_server(
        self,
        _deployment_name: str,
        _location_name: str,
        _server_handle: ServerHandle,
        multipex_endpoint: ServerEndpoint,
    ):
        self._wait_for_server_process(
            multipex_endpoint.create_multipex_client(),
            timeout=self._server_process_startup_timeout,
        )

    @abstractmethod
    def _wait_for_new_server_ready(
        self,
        deployment_name: str,
        location_name: str,
        desired_entry: UserCodeLauncherEntry,
        server_handle: ServerHandle,
        server_endpoint: ServerEndpoint,
    ) -> None:
        """Wait for a newly-created server to be ready."""

    def _remove_pex_server_handle(
        self,
        _deployment_name,
        _location_name,
        _server_handle: ServerHandle,
        server_endpoint: ServerEndpoint,
        pex_server_handle: PexServerHandle,
    ) -> None:
        multi_pex_client = server_endpoint.create_multipex_client()
        multi_pex_client.shutdown_pex_server(ShutdownPexServerArgs(server_handle=pex_server_handle))

    @abstractmethod
    def _remove_server_handle(self, server_handle: ServerHandle) -> None:
        """Shut down any resources associated with the given handle. Called both during updates
        to spin down the old server once a new server has been spun up, and during removal.
        """

    @property
    def supports_get_current_runs_for_server_handle(self) -> bool:
        return False

    def get_current_runs_for_server_handle(self, server_handle: ServerHandle) -> Sequence[str]:
        raise NotImplementedError()

    def _graceful_remove_server_handle(self, server_handle: ServerHandle):
        """Check if there are non isolated runs and wait for them to finish before shutting down
        the server.
        """
        if not self.supports_get_current_runs_for_server_handle:
            return self._remove_server_handle(server_handle)

        run_ids = None
        try:
            run_ids = self.get_current_runs_for_server_handle(server_handle)
        except Exception:
            self._logger.error(
                f"Failure connecting to server with handle {server_handle}, going to shut it down:"
                f" {serializable_error_info_from_exc_info(sys.exc_info())}"
            )

        if run_ids:
            self._logger.info(
                f"Waiting for run_ids [{', '.join(run_ids)}] to finish before shutting down server"
                f" {server_handle}"
            )
            with self._grpc_servers_lock:
                self._pending_delete_grpc_server_handles.add(server_handle)
        else:
            if run_ids == []:  # If it's None, the grpc call failed
                self._logger.info(f"No runs, shutting down server {server_handle}")
            self._remove_server_handle(server_handle)
            with self._grpc_servers_lock:
                self._pending_delete_grpc_server_handles.discard(server_handle)

    def _cleanup_servers(self, active_agent_ids: Set[str], include_own_servers: bool) -> None:
        """Remove all servers, across all deployments and locations."""
        with ThreadPoolExecutor() as executor:
            futures = []
            for handle in self._list_server_handles():
                self._logger.info(f"Attempting to cleanup server {handle}")
                if self._can_cleanup_server(
                    handle, active_agent_ids, include_own_servers=include_own_servers
                ):
                    self._logger.info(f"Can remove server {handle}. Cleaning up.")
                    futures.append(executor.submit(self._remove_server_handle, handle))
                else:
                    self._logger.info(f"Cannot remove server {handle}. Not cleaning up.")

            wait(futures)
            for future in futures:
                try:
                    future.result()
                except:
                    self._logger.exception("Error cleaning up server")

    @abstractmethod
    def _list_server_handles(self) -> List[ServerHandle]:
        """Return a list of all server handles across all deployments and locations."""

    @abstractmethod
    def get_agent_id_for_server(self, handle: ServerHandle) -> Optional[str]:
        """Returns the agent_id that created a particular GRPC server."""

    @abstractmethod
    def get_server_create_timestamp(self, handle: ServerHandle) -> Optional[float]:
        """Returns the update_timestamp value from the given code server."""

    def _can_cleanup_server(
        self, handle: ServerHandle, active_agent_ids: Set[str], include_own_servers: bool
    ) -> bool:
        """Returns true if we can clean up the server identified by the handle without issues (server was started by this agent, or agent is no longer active)."""
        agent_id_for_server = self.get_agent_id_for_server(handle)
        self._logger.info(
            f"For server {handle}; agent_id is {agent_id_for_server} while current agent_id is"
            f" {self._instance.instance_uuid}."
        )
        self._logger.info(f"All active agent ids: {active_agent_ids}")

        # if it's a legacy server that never set an agent ID:
        if not agent_id_for_server:
            return True

        if self._instance.instance_uuid == agent_id_for_server:
            return include_own_servers

        try:
            update_timestamp_for_server = self.get_server_create_timestamp(handle)
        except:
            self._logger.exception(f"Failure fetching service creation timestamp for {handle}")
            return False

        # Clean up servers that were created more than CLEANUP_SERVER_GRACE_PERIOD_SECONDS
        # seconds ago (to avoid race conditions) and were created by some agent that is now
        # inactive, to ensure that servers are eventually cleaned up by the next agent
        # when an agent crashes
        if (
            update_timestamp_for_server
            and update_timestamp_for_server
            >= pendulum.now("UTC").timestamp() - CLEANUP_SERVER_GRACE_PERIOD_SECONDS
        ):
            self._logger.info("Not cleaning up server since it was recently created")
            return False

        return agent_id_for_server not in cast(Set[str], active_agent_ids)

    def _graceful_cleanup_servers(self, include_own_servers: bool):  # ServerHandles
        active_agent_ids = self.get_active_agent_ids()
        if not self.supports_get_current_runs_for_server_handle:
            return self._cleanup_servers(active_agent_ids, include_own_servers=include_own_servers)

        handles = self._list_server_handles()
        servers_to_remove: List[ServerHandle] = []
        with self._grpc_servers_lock:
            for handle in handles:
                if self._can_cleanup_server(
                    handle, active_agent_ids, include_own_servers=include_own_servers
                ):
                    servers_to_remove.append(handle)
            self._pending_delete_grpc_server_handles.update(servers_to_remove)
        for server_handle in servers_to_remove:
            self._graceful_remove_server_handle(server_handle)

    @abstractmethod
    def run_launcher(self) -> RunLauncher:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self._reconcile_grpc_metadata_thread:
            self._reconcile_grpc_metadata_shutdown_event.set()
            self._reconcile_grpc_metadata_thread.join()

        if self._run_worker_monitoring_thread:
            self._run_worker_monitoring_thread_shutdown_event.set()
            self._run_worker_monitoring_thread.join()

        if self._reconcile_location_utilization_metrics_thread:
            self._reconcile_location_utilization_metrics_shutdown_event.set()
            self._reconcile_location_utilization_metrics_thread.join()

        if self._started:
            self._graceful_cleanup_servers(include_own_servers=True)

        super().__exit__(exception_value, exception_value, traceback)

    def add_upload_metadata(
        self, upload_metadata: Dict[DeploymentAndLocation, UserCodeLauncherEntry]
    ):
        """Add a set of locations to be uploaded in the next reconcilation loop."""
        with self._metadata_lock:
            self._upload_locations = self._upload_locations.union(upload_metadata)
            self._desired_entries = merge_dicts(self._desired_entries, upload_metadata)

    def update_grpc_metadata(
        self,
        desired_metadata: Dict[DeploymentAndLocation, UserCodeLauncherEntry],
    ):
        check.dict_param(
            desired_metadata,
            "desired_metadata",
            key_type=tuple,
            value_type=UserCodeLauncherEntry,
        )

        with self._metadata_lock:
            # Need to be careful here to not wipe out locations that are marked to be uploaded
            # before they get a chance to be uploaded - make sure those locations and their
            # metadata don't get removed until the upload happens
            keys_to_keep = self._upload_locations.difference(desired_metadata)
            metadata_to_keep = {
                key_to_keep: self._desired_entries[key_to_keep] for key_to_keep in keys_to_keep
            }
            self._desired_entries = merge_dicts(metadata_to_keep, desired_metadata)

    @abstractmethod
    def get_code_server_resource_limits(
        self, deployment_name: str, location_name: str
    ) -> CloudContainerResourceLimits:
        pass

    def record_resource_limit_metrics_all_locations(self):
        for deployment_name, location_name in self._actual_entries.keys():
            if self.code_server_metrics_enabled:
                metadata = self.get_code_server_resource_limits(deployment_name, location_name)
                self._per_location_metrics[(deployment_name, location_name)]["resource_limits"] = (
                    metadata
                )
                self._logger.info(
                    f"Updated resource limits for location {location_name} in deployment {deployment_name}: {metadata}"
                )

    def update_utilization_metrics_all_locations(self):
        endpoints_or_errors = self.get_grpc_endpoints()
        for (deployment_name, location_name), endpoint_or_error in endpoints_or_errors.items():
            if isinstance(endpoint_or_error, ServerEndpoint):
                endpoint = cast(ServerEndpoint, endpoint_or_error)
                raw_metrics_str = (
                    endpoint.create_client().ping("").get("serialized_server_utilization_metrics")
                )
                if not raw_metrics_str or raw_metrics_str == "":
                    continue
                metrics = json.loads(raw_metrics_str)
                self._logger.info(
                    f"Updated code server metrics for location {location_name} in deployment {deployment_name}: {metrics}"
                )
                for key, val in metrics.items():
                    self._per_location_metrics[(deployment_name, location_name)][key] = val

    def _get_code_location_origin(self, location_name: str) -> RegisteredCodeLocationOrigin:
        return RegisteredCodeLocationOrigin(location_name)

    @property
    def _reconcile_interval(self):
        return PENDING_DELETE_SERVER_CHECK_INTERVAL

    def _reconcile_thread(self, shutdown_event):
        while True:
            shutdown_event.wait(USER_CODE_LAUNCHER_RECONCILE_SLEEP_SECONDS)
            if shutdown_event.is_set():
                break

            try:
                self.reconcile()
            except Exception:
                self._logger.error(
                    f"Failure updating user code servers: {serializable_error_info_from_exc_info(sys.exc_info())}"
                )

    def _cleanup_server_check_interval(self):
        return int(os.getenv("DAGSTER_CLOUD_CLEANUP_SERVER_CHECK_INTERVAL", "1800"))

    def reconcile(self) -> None:
        with self._metadata_lock:
            desired_entries = (
                self._desired_entries.copy() if self._desired_entries is not None else None
            )
            upload_locations = self._upload_locations.copy()
            self._upload_locations = set()

        if desired_entries is None:
            # Wait for the first time the desired metadata is set before reconciling
            return

        now = pendulum.now("UTC").timestamp()

        if not self._last_refreshed_actual_entries:
            self._last_refreshed_actual_entries = now

        if not self._last_cleaned_up_dangling_code_servers:
            self._last_cleaned_up_dangling_code_servers = now

        cleanup_server_check_interval = self._cleanup_server_check_interval()

        if (
            cleanup_server_check_interval
            and now - self._last_cleaned_up_dangling_code_servers > cleanup_server_check_interval
        ):
            try:
                self._graceful_cleanup_servers(include_own_servers=False)
            except:
                self._logger.exception("Failed to clean up dangling code serverrs.")
            self._last_cleaned_up_dangling_code_servers = now

        if now - self._last_refreshed_actual_entries > ACTUAL_ENTRIES_REFRESH_INTERVAL:
            try:
                self._refresh_actual_entries()
            except:
                self._logger.exception("Failed to refresh actual entries.")
            self._last_refreshed_actual_entries = now

        self._reconcile(
            desired_entries,
            upload_locations,
            check_on_pending_delete_servers=self._reconcile_count % self._reconcile_interval == 0,
        )
        if self._reconcile_count == 0 and self._requires_healthcheck:
            try:
                self._write_liveness_sentinel()
            except:
                self._logger.exception("Failed to write liveness sentinel file.")

        if self._reconcile_count == 0:
            self._logger.info(
                f"Started polling for requests from {self._instance.dagster_cloud_url}"
            )

        self._reconcile_count += 1

    def _update_metrics_thread(self, shutdown_event):
        while True:
            shutdown_event.wait(USER_CODE_LAUNCHER_RECONCILE_METRICS_SLEEP_SECONDS)
            if shutdown_event.is_set():
                break

            try:
                self.record_resource_limit_metrics_all_locations()
                self.update_utilization_metrics_all_locations()
                self._logger.info(
                    f"Current code server utilization metrics: {self._per_location_metrics}"
                )
            except Exception:
                self._logger.error(
                    f"Failure updating user code server metrics: {serializable_error_info_from_exc_info(sys.exc_info())}"
                )

    @property
    def ready_to_serve_requests(self) -> bool:
        # thread-safe since reconcile_count is an integer
        return self._reconcile_count > 0

    def _make_check_on_running_server_endpoint(self, server_endpoint: ServerEndpoint):
        # Ensure that server_endpoint is bound correctly
        return lambda: server_endpoint.create_client().ping("")

    def _refresh_actual_entries(self) -> None:
        for deployment_location, server in self._multipex_servers.items():
            if deployment_location in self._actual_entries:
                # If a multipex server exists, we query it over gRPC
                # to make sure the pex server is still available.

                # First verify that the multipex server is running
                try:
                    server.server_endpoint.create_multipex_client().ping("")
                except:
                    # If it isn't, this is expected if ECS is currently spinning up this service
                    # after it crashed. In this case, we want to wait for it to fully come up
                    # before we remove actual entries. This ensures the recon loop uses the ECS
                    # replacement multipex server and not try to spin up a new multipex server.
                    self._logger.info(
                        "Multipex server entry exists but server is not running. "
                        "Will wait for server to come up."
                    )
                    return
                deployment_name, location_name = deployment_location
                if not self._get_existing_pex_servers(deployment_name, location_name):
                    self._logger.warning(
                        "Pex servers disappeared for %s:%s. Removing actual entries to"
                        " activate reconciliation logic.",
                        deployment_name,
                        location_name,
                    )
                    del self._actual_entries[deployment_location]

        # Check to see if any servers have become unresponsive
        unavailable_server_timeout = int(
            os.getenv(
                "DAGSTER_CLOUD_CODE_SERVER_HEALTH_CHECK_REDEPLOY_TIMEOUT",
                str(self._server_process_startup_timeout),
            )
        )

        if unavailable_server_timeout < 0:
            return

        running_locations = {
            deployment_location: endpoint_or_error
            for deployment_location, endpoint_or_error in self.get_grpc_endpoints().items()
            if (
                isinstance(endpoint_or_error, ServerEndpoint)
                and deployment_location in self._actual_entries
            )
        }

        if not running_locations:
            return

        with ThreadPoolExecutor(
            max_workers=max(
                len(running_locations),
                int(os.getenv("DAGSTER_CLOUD_CODE_SERVER_HEALTH_CHECK_MAX_WORKERS", "8")),
            ),
            thread_name_prefix="dagster_cloud_agent_server_health_check",
        ) as executor:
            futures = {}
            for deployment_location, endpoint_or_error in running_locations.items():
                deployment_name, location_name = deployment_location

                futures[
                    executor.submit(self._make_check_on_running_server_endpoint(endpoint_or_error))
                ] = deployment_location

            for future in as_completed(futures):
                deployment_location = futures[future]

                deployment_name, location_name = deployment_location
                try:
                    future.result()

                    # Successful ping resets the tracked last unavailable time for this code server, if set
                    self._first_unavailable_times.pop(deployment_location, None)
                except Exception as e:
                    if (
                        isinstance(e, DagsterUserCodeUnreachableError)
                        and isinstance(e.__cause__, grpc.RpcError)
                        and cast(grpc.RpcError, e.__cause__).code() == grpc.StatusCode.UNAVAILABLE
                    ):
                        first_unavailable_time = self._first_unavailable_times.get(
                            deployment_location
                        )

                        now = pendulum.now("UTC").timestamp()

                        if not first_unavailable_time:
                            self._logger.warning(
                                f"Code server for {deployment_name}:{location_name} failed a health check. If it continues failing for more than {unavailable_server_timeout} seconds, a replacement code server will be deployed."
                            )
                            # Initialize the first unavailable time if set
                            self._first_unavailable_times[deployment_location] = now
                        elif now > first_unavailable_time + unavailable_server_timeout:
                            self._logger.warning(
                                f"Code server for {deployment_name}:{location_name} has been unresponsive for more than {unavailable_server_timeout} seconds. Deploying a new code server."
                            )
                            del self._actual_entries[deployment_location]
                            del self._first_unavailable_times[deployment_location]
                    else:
                        self._logger.exception(
                            f"Code server for {deployment_name}:{location_name} health check failed, but the error did not indicate that the server was unavailable."
                        )
                        self._first_unavailable_times.pop(deployment_location, None)

    def _write_liveness_sentinel(self) -> None:
        """Write a sentinel file to indicate that the agent is alive and grpc servers have been spun up."""
        pass

    def _check_for_image(self, metadata: CodeDeploymentMetadata):
        if self.requires_images and not self._resolve_image(metadata):
            raise Exception(
                "Your agent's configuration requires you to specify an image. "
                "Use the `--image` flag when specifying your location to tell the agent "
                "which image to use to load your code."
            )

        if (not self.requires_images) and metadata.image:
            raise Exception(
                "Your agent's configuration cannot load locations that specify a Docker image."
                " Either update your location to not include an image, or change the"
                " `user_code_launcher` field in your agent's `dagster.yaml` file to a launcher that"
                " can load Docker images. "
            )

    def _deployments_and_locations_to_string(
        self,
        deployments_and_locations: Set[DeploymentAndLocation],
        entries: Dict[DeploymentAndLocation, UserCodeLauncherEntry],
    ):
        return (
            "{"
            + ", ".join(
                sorted(
                    [
                        f"({dep}, {loc}, {entries[(dep,loc)].update_timestamp})"
                        for dep, loc in deployments_and_locations
                    ]
                )
            )
            + "}"
        )

    def _check_running_multipex_server(self, multipex_server: DagsterCloudGrpcServer):
        multipex_server.server_endpoint.create_multipex_client().ping("")

    def _reconcile(
        self,
        desired_entries: Dict[DeploymentAndLocation, UserCodeLauncherEntry],
        upload_locations: Set[DeploymentAndLocation],
        check_on_pending_delete_servers: bool,
    ):
        if check_on_pending_delete_servers:
            with self._grpc_servers_lock:
                handles = self._pending_delete_grpc_server_handles.copy()
            if handles:
                self._logger.info("Checking on pending delete servers")
            for handle in handles:
                self._graceful_remove_server_handle(handle)

        diff = diff_serializable_namedtuple_map(desired_entries, self._actual_entries)
        has_changes = diff.to_add or diff.to_update or diff.to_remove or upload_locations

        if not has_changes:
            return

        goal_str = self._deployments_and_locations_to_string(
            set(desired_entries.keys()), desired_entries
        )
        to_add_str = self._deployments_and_locations_to_string(diff.to_add, desired_entries)
        to_update_str = self._deployments_and_locations_to_string(diff.to_update, desired_entries)
        to_remove_str = self._deployments_and_locations_to_string(
            diff.to_remove, self._actual_entries
        )
        to_upload_str = self._deployments_and_locations_to_string(upload_locations, desired_entries)

        start_time = time.time()

        self._logger.info(
            f"Reconciling to reach {goal_str}. To add: {to_add_str}. To update: {to_update_str}. To"
            f" remove: {to_remove_str}. To upload: {to_upload_str}."
        )

        to_update_keys = diff.to_add.union(diff.to_update)

        # Handles for all running standalone Dagster GRPC servers
        existing_standalone_dagster_server_handles: Dict[
            DeploymentAndLocation, Collection[ServerHandle]
        ] = {}

        # Handles for all running Dagster multipex servers (which can each host multiple grpc subprocesses)
        existing_multipex_server_handles: Dict[DeploymentAndLocation, Collection[ServerHandle]] = {}

        # For each location, all currently running pex servers on the current multipex server
        existing_pex_server_handles: Dict[DeploymentAndLocation, List[PexServerHandle]] = {}

        # Dagster grpc servers created in this loop (including both standalone grpc servers
        # and pex servers on a multipex server) - or an error that explains why it couldn't load
        new_dagster_servers: Dict[
            DeploymentAndLocation, Union[DagsterCloudGrpcServer, SerializableErrorInfo]
        ] = {}

        # Multipex servers created in this loop (a new multipex server might not always
        # be created on each loop even if the code has changed, as long as the base image
        # is the same)
        new_multipex_servers: Dict[DeploymentAndLocation, DagsterCloudGrpcServer] = {}

        for to_update_key in to_update_keys:
            deployment_name, location_name = to_update_key

            desired_entry = desired_entries[to_update_key]

            code_deployment_metadata = desired_entry.code_deployment_metadata

            # First check what multipex servers already exist for this location (any that are
            # no longer used will be cleaned up at the end)
            existing_multipex_server_handles[to_update_key] = (
                self._get_multipex_server_handles_for_location(deployment_name, location_name)
            )

            if code_deployment_metadata.pex_metadata:
                try:
                    # See if a multipex server exists that satisfies this new metadata or if
                    # one needs to be created
                    multipex_server = self._get_multipex_server(
                        deployment_name, location_name, desired_entry.code_deployment_metadata
                    )

                    if multipex_server:
                        try:
                            self._check_running_multipex_server(multipex_server)
                        except:
                            error_info = serializable_error_info_from_exc_info(sys.exc_info())
                            self._logger.error(
                                "Spinning up a new multipex server for"
                                f" {deployment_name}:{location_name} since the existing one failed"
                                f" with the following error: {error_info}"
                            )
                            multipex_server = None

                    desired_pex_metadata = desired_entry.code_deployment_metadata.pex_metadata
                    desired_python_version = (
                        desired_pex_metadata.python_version if desired_pex_metadata else None
                    )
                    multipex_server_repr = f"{deployment_name}:{location_name} image={desired_entry.code_deployment_metadata.image} python_version={desired_python_version}"
                    if not multipex_server:
                        self._logger.info(
                            f"Creating new multipex server for {multipex_server_repr}"
                        )
                        # confirm it's a valid image since _start_new_server_spinup will launch a container
                        self._check_for_image(desired_entry.code_deployment_metadata)
                        multipex_server = self._start_new_server_spinup(
                            deployment_name, location_name, desired_entry
                        )
                        self._multipex_servers[to_update_key] = multipex_server
                        assert self._get_multipex_server(
                            deployment_name,
                            location_name,
                            desired_entry.code_deployment_metadata,
                        )
                        new_multipex_servers[to_update_key] = multipex_server
                    else:
                        self._logger.info(
                            f"Found running multipex server for {multipex_server_repr}"
                        )

                except Exception:
                    error_info = serializable_error_info_from_exc_info(sys.exc_info())
                    self._logger.error(
                        "Error while setting up multipex server for"
                        f" {deployment_name}:{location_name}: {error_info}"
                    )
                    new_dagster_servers[to_update_key] = error_info
            elif to_update_key in self._multipex_servers:
                # This key is no longer a multipex server
                del self._multipex_servers[to_update_key]

        # For each new multi-pex server, wait for it to be ready. If it fails, put
        # the location that was planned to use it into an error state
        for to_update_key, multipex_server in new_multipex_servers.items():
            deployment_name, location_name = to_update_key

            try:
                self._logger.info(
                    f"Waiting for new multipex server for {deployment_name}:{location_name} to be"
                    " ready"
                )
                self._wait_for_new_multipex_server(
                    deployment_name,
                    location_name,
                    multipex_server.server_handle,
                    multipex_server.server_endpoint,
                )
            except Exception:
                error_info = serializable_error_info_from_exc_info(sys.exc_info())

                self._logger.error(
                    f"Error while waiting for multipex server for {deployment_name}:{location_name}:"
                    f" {error_info}"
                )
                new_dagster_servers[to_update_key] = error_info
                # Clear out this multipex server so we don't try to use it again
                del self._multipex_servers[to_update_key]

        # Now that any needed multipex servers have been created, spin up dagster servers
        # (either as standalone servers or within a multipex server)
        for to_update_key in to_update_keys:
            if isinstance(new_dagster_servers.get(to_update_key), SerializableErrorInfo):
                # Don't keep going for this location if a previous step failed
                continue

            deployment_name, location_name = to_update_key
            try:
                desired_entry = desired_entries[to_update_key]
                code_deployment_metadata = desired_entry.code_deployment_metadata

                self._logger.info(f"Updating server for {deployment_name}:{location_name}")
                existing_standalone_dagster_server_handles[to_update_key] = (
                    self._get_standalone_dagster_server_handles_for_location(
                        deployment_name, location_name
                    )
                )

                existing_pex_server_handles[to_update_key] = self._get_existing_pex_servers(
                    deployment_name, location_name
                )

                self._check_for_image(code_deployment_metadata)

                new_dagster_servers[to_update_key] = self._start_new_dagster_server(
                    deployment_name,
                    location_name,
                    desired_entry,
                )

                self._logger.info(f"Created a new server for {to_update_key}")
            except Exception:
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error(
                    f"Error while updating server for {deployment_name}:{location_name}:"
                    f" {error_info}"
                )
                new_dagster_servers[to_update_key] = error_info

        # Wait for all new dagster servers (standalone or within a multipex server) to be ready
        for to_update_key in to_update_keys:
            deployment_name, location_name = to_update_key
            code_deployment_metadata = desired_entries[to_update_key].code_deployment_metadata
            server_or_error = new_dagster_servers[to_update_key]

            pex_metadata = code_deployment_metadata.pex_metadata
            deployment_info = (
                f"(pex_tag={pex_metadata.pex_tag}, python_version={pex_metadata.python_version})"
                if pex_metadata
                else f"(image={code_deployment_metadata})"
            )
            if not isinstance(server_or_error, SerializableErrorInfo):
                try:
                    self._logger.info(
                        f"Waiting for new grpc server for {to_update_key} for {deployment_info} to be ready..."
                    )
                    self._wait_for_new_server_ready(
                        deployment_name,
                        location_name,
                        desired_entries[to_update_key],
                        server_or_error.server_handle,
                        server_or_error.server_endpoint,
                    )
                except Exception:
                    error_info = serializable_error_info_from_exc_info(sys.exc_info())
                    self._logger.error(
                        f"Error while waiting for server for {deployment_name}:{location_name} for {deployment_info} to be"
                        f" ready: {error_info}"
                    )
                    server_or_error = error_info

            # If needed, upload snapshot information to Dagster Cloud
            if to_update_key in upload_locations:
                upload_locations.remove(to_update_key)
                try:
                    self._update_location_data(
                        deployment_name,
                        location_name,
                        server_or_error,
                        desired_entries[to_update_key].code_deployment_metadata,
                    )
                except Exception:
                    # If there was a failure uploading snapshots, log it but don't block other code locations
                    # from updating (and still use the new server to serve new requests)
                    error_info = serializable_error_info_from_exc_info(sys.exc_info())
                    self._logger.error(
                        f"Error while writing location data for {deployment_name}:{location_name}:"
                        f" {error_info}"
                    )

            # Once we've verified that the new server has uploaded its data successfully, swap in
            # the server to start serving new requests
            with self._grpc_servers_lock:
                self._grpc_servers[to_update_key] = server_or_error
                self._first_unavailable_times.pop(to_update_key, None)

        for to_update_key in to_update_keys:
            deployment_name, location_name = to_update_key

            # Remove any old standalone grpc server containers
            server_handles = existing_standalone_dagster_server_handles.get(to_update_key, [])
            removed_any_servers = False

            if server_handles:
                removed_any_servers = True
                self._logger.info(
                    f"Removing {len(server_handles)} existing servers for {deployment_name}:{location_name}"
                )

            for server_handle in server_handles:
                try:
                    self._graceful_remove_server_handle(server_handle)
                except Exception:
                    self._logger.error(
                        "Error while cleaning up after updating server for"
                        f" {deployment_name}:{location_name}: {serializable_error_info_from_exc_info(sys.exc_info())}"
                    )

            # Remove any existing multipex servers other than the current one for each location
            multipex_server_handles = existing_multipex_server_handles.get(to_update_key, [])

            current_multipex_server = self._get_multipex_server(
                deployment_name,
                location_name,
                desired_entries[to_update_key].code_deployment_metadata,
            )

            for multipex_server_handle in multipex_server_handles:
                current_multipex_server_handle = (
                    current_multipex_server.server_handle if current_multipex_server else None
                )

                if (
                    not current_multipex_server_handle
                    or current_multipex_server_handle != multipex_server_handle
                ):
                    self._logger.info(
                        f"Removing old multipex server for {deployment_name}:{location_name}"
                    )

                    try:
                        self._graceful_remove_server_handle(multipex_server_handle)
                    except Exception:
                        self._logger.error(
                            "Error while cleaning up old multipex server for"
                            f" {deployment_name}:{location_name}: {serializable_error_info_from_exc_info(sys.exc_info())}"
                        )

            # On the current multipex server, shut down any old pex servers
            pex_server_handles = existing_pex_server_handles.get(to_update_key)
            if current_multipex_server and pex_server_handles:
                removed_any_servers = True
                self._logger.info(
                    f"Removing {len(pex_server_handles)} grpc processes from multipex server for"
                    f" {deployment_name}:{location_name}"
                )
                for pex_server_handle in pex_server_handles:
                    try:
                        self._remove_pex_server_handle(
                            deployment_name,
                            location_name,
                            current_multipex_server.server_handle,
                            current_multipex_server.server_endpoint,
                            pex_server_handle,
                        )
                    except Exception:
                        self._logger.error(
                            "Error while cleaning up after updating server for"
                            f" {deployment_name}:{location_name}: {serializable_error_info_from_exc_info(sys.exc_info())}"
                        )

            if removed_any_servers:
                self._logger.info(
                    f"Removed all previous servers for {deployment_name}:{location_name}"
                )

            # Always update our actual entries
            self._actual_entries[to_update_key] = desired_entries[to_update_key]

        for to_remove_key in diff.to_remove:
            deployment_name, location_name = to_remove_key
            try:
                self._remove_server(deployment_name, location_name)
            except Exception:
                self._logger.error(
                    f"Error while removing server for {deployment_name}:{location_name}:"
                    f" {serializable_error_info_from_exc_info(sys.exc_info())}"
                )

            with self._grpc_servers_lock:
                del self._grpc_servers[to_remove_key]
            del self._actual_entries[to_remove_key]

        # Upload any locations that were requested to be uploaded, but weren't updated
        # as part of this reconciliation loop
        for location in upload_locations:
            with self._grpc_servers_lock:
                server_or_error = self._grpc_servers[location]

            deployment_name, location_name = location
            try:
                self._update_location_data(
                    deployment_name,
                    location_name,
                    server_or_error,
                    self._actual_entries[location].code_deployment_metadata,
                )
            except Exception:
                self._logger.error(
                    f"Error while writing location data for {deployment_name}:{location_name}:"
                    f" {serializable_error_info_from_exc_info(sys.exc_info())}"
                )

        seconds = time.time() - start_time
        self._logger.info(f"Finished reconciling in {seconds} seconds.")
        self._event_logger.info(
            "user_code_launcher.RECONCILED",
            {"event_name": "user_code_launcher.RECONCILED", "duration_seconds": seconds},
        )

    def has_grpc_endpoint(self, deployment_name: str, location_name: str) -> bool:
        with self._grpc_servers_lock:
            return (deployment_name, location_name) in self._grpc_servers

    def _get_multipex_server(
        self,
        deployment_name,
        location_name,
        code_deployment_metadata,
    ) -> Optional[DagsterCloudGrpcServer]:
        if not code_deployment_metadata.pex_metadata:
            return None

        cand_server = self._multipex_servers.get((deployment_name, location_name))

        if not cand_server:
            return None

        cand_python_version = (
            cand_server.code_deployment_metadata.pex_metadata.python_version
            if cand_server.code_deployment_metadata.pex_metadata
            else None
        )
        python_version = (
            code_deployment_metadata.pex_metadata.python_version
            if code_deployment_metadata.pex_metadata
            else None
        )
        if (
            (cand_server.code_deployment_metadata.image == code_deployment_metadata.image)
            and (cand_python_version == python_version)
            and (
                cand_server.code_deployment_metadata.container_context
                == code_deployment_metadata.container_context
            )
        ):
            return cand_server

        return None

    def _create_pex_server(
        self,
        deployment_name: str,
        location_name: str,
        desired_entry: UserCodeLauncherEntry,
        multipex_server: DagsterCloudGrpcServer,
    ):
        multipex_endpoint = multipex_server.server_endpoint
        multipex_client = multipex_endpoint.create_multipex_client()
        multipex_client.create_pex_server(
            CreatePexServerArgs(
                server_handle=PexServerHandle(
                    deployment_name=deployment_name,
                    location_name=location_name,
                    metadata_update_timestamp=int(desired_entry.update_timestamp),
                ),
                code_deployment_metadata=desired_entry.code_deployment_metadata,
                instance_ref=self._instance.ref_for_deployment(deployment_name),
            )
        )

    def _start_new_dagster_server(
        self, deployment_name: str, location_name: str, desired_entry: UserCodeLauncherEntry
    ) -> DagsterCloudGrpcServer:
        if desired_entry.code_deployment_metadata.pex_metadata:
            multipex_server = self._get_multipex_server(
                deployment_name, location_name, desired_entry.code_deployment_metadata
            )

            assert multipex_server  # should have been started earlier or we should never reach here

            self._create_pex_server(deployment_name, location_name, desired_entry, multipex_server)

            server_handle = multipex_server.server_handle
            multipex_endpoint = multipex_server.server_endpoint

            # start a new pex server on the multipexer, which we can count on already existing
            return DagsterCloudGrpcServer(
                server_handle,
                multipex_endpoint.with_metadata(
                    [
                        ("has_pex", "1"),
                        ("deployment", deployment_name),
                        ("location", location_name),
                        ("timestamp", str(int(desired_entry.update_timestamp))),
                    ],
                ),
                desired_entry.code_deployment_metadata,
            )
        else:
            return self._start_new_server_spinup(deployment_name, location_name, desired_entry)

    def get_grpc_endpoint(
        self,
        deployment_name: str,
        location_name: str,
    ) -> ServerEndpoint:
        with self._grpc_servers_lock:
            server = self._grpc_servers.get((deployment_name, location_name))

        if not server:
            raise DagsterUserCodeUnreachableError(
                f"No server endpoint exists for {deployment_name}:{location_name}"
            )

        if isinstance(server, SerializableErrorInfo):
            # Consider raising the original exception here instead of a wrapped one
            raise DagsterUserCodeUnreachableError(
                f"Failure loading server endpoint for {deployment_name}:{location_name}:\n{server}"
            )

        return server.server_endpoint

    def get_grpc_server(
        self,
        deployment_name: str,
        location_name: str,
    ) -> DagsterCloudGrpcServer:
        with self._grpc_servers_lock:
            server = self._grpc_servers.get((deployment_name, location_name))

        if not server:
            raise DagsterUserCodeUnreachableError(
                f"No server endpoint exists for {deployment_name}:{location_name}"
            )

        if isinstance(server, SerializableErrorInfo):
            # Consider raising the original exception here instead of a wrapped one
            raise DagsterUserCodeUnreachableError(
                f"Failure loading server endpoint for {deployment_name}:{location_name}:\n{server}"
            )

        return server

    def get_grpc_server_heartbeats(self) -> Dict[str, List[CloudCodeServerHeartbeat]]:
        endpoint_or_errors = self.get_grpc_endpoints()
        with self._metadata_lock:
            desired_entries = set(self._desired_entries.keys())

        heartbeats: Dict[str, List[CloudCodeServerHeartbeat]] = {}
        for entry_key in desired_entries:
            deployment_name, location_name = entry_key
            endpoint_or_error = endpoint_or_errors.get(entry_key)
            metadata: CloudCodeServerHeartbeatMetadata = {}
            if self.code_server_metrics_enabled:
                metadata["utilization_metrics"] = self._per_location_metrics.get(
                    entry_key, init_optional_typeddict(CloudCodeServerUtilizationMetrics)
                )

            error = (
                endpoint_or_error if isinstance(endpoint_or_error, SerializableErrorInfo) else None
            )

            if error:
                status = CloudCodeServerStatus.FAILED
            elif endpoint_or_error:
                status = CloudCodeServerStatus.RUNNING
            else:
                # no endpoint yet means it's still being created
                status = CloudCodeServerStatus.STARTING

            if deployment_name not in heartbeats:
                heartbeats[deployment_name] = []

            heartbeats[deployment_name].append(
                CloudCodeServerHeartbeat(
                    location_name,
                    server_status=status,
                    error=(
                        endpoint_or_error
                        if isinstance(endpoint_or_error, SerializableErrorInfo)
                        else None
                    ),
                    metadata=metadata,
                )
            )

        return heartbeats

    def get_grpc_endpoints(
        self,
    ) -> Dict[DeploymentAndLocation, Union[ServerEndpoint, SerializableErrorInfo]]:
        with self._grpc_servers_lock:
            return {
                key: val if isinstance(val, SerializableErrorInfo) else val.server_endpoint
                for key, val in self._grpc_servers.items()
            }

    def _remove_server(self, deployment_name: str, location_name: str):
        self._logger.info(f"Removing server for {deployment_name}:{location_name}")
        existing_standalone_dagster_server_handles = (
            self._get_standalone_dagster_server_handles_for_location(deployment_name, location_name)
        )
        for server_handle in existing_standalone_dagster_server_handles:
            self._graceful_remove_server_handle(server_handle)

        existing_multipex_server_handles = self._get_multipex_server_handles_for_location(
            deployment_name, location_name
        )
        for server_handle in existing_multipex_server_handles:
            self._graceful_remove_server_handle(server_handle)

    def _wait_for_dagster_server_process(
        self,
        client: DagsterGrpcClient,
        timeout,
        additional_check: Optional[Callable[[], None]] = None,
        get_timeout_debug_info: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._wait_for_server_process(
            client, timeout, additional_check, get_timeout_debug_info=get_timeout_debug_info
        )
        # Call a method that raises an exception if there was an error importing the code
        sync_list_repositories_grpc(client)

    def _wait_for_server_process(
        self,
        client: Union[DagsterGrpcClient, MultiPexGrpcClient],
        timeout,
        additional_check: Optional[Callable[[], None]] = None,
        additional_check_interval: int = 5,
        get_timeout_debug_info: Optional[Callable[[], None]] = None,
    ) -> None:
        start_time = time.time()

        last_error = None

        last_additional_check_time = None

        while True:
            try:
                client.ping("")
                break
            except Exception:
                last_error = serializable_error_info_from_exc_info(sys.exc_info())

            if time.time() - start_time > timeout:
                timeout_debug_info = ""
                if get_timeout_debug_info:
                    try:
                        timeout_debug_info = get_timeout_debug_info()
                    except Exception:
                        self._logger.exception("Failure fetching debug info after a timeout")

                raise Exception(
                    f"Timed out after waiting {timeout}s for server"
                    f" {client.host}:{client.port or client.socket}."
                    + (f"\n\n{timeout_debug_info}" if timeout_debug_info else "")
                    + f"\n\nMost recent connection error: {last_error}"
                )

            time.sleep(1)

            if additional_check and (
                not last_additional_check_time
                or time.time() - last_additional_check_time > additional_check_interval
            ):
                last_additional_check_time = time.time()
                additional_check()

    def upload_job_snapshot(
        self,
        deployment_name: str,
        job_selector: JobSelector,
        server: DagsterCloudGrpcServer,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = server.server_endpoint.create_client()
            location_origin = self._get_code_location_origin(job_selector.location_name)
            response = client.external_job(
                RemoteRepositoryOrigin(location_origin, job_selector.repository_name),
                job_selector.job_name,
            )
            if not response.serialized_job_data:
                error = (
                    deserialize_value(response.serialized_error, SerializableErrorInfo)
                    if response.serialized_error
                    else "no captured error"
                )
                raise Exception(f"Error fetching job data in code server:\n{error}")

            dst = os.path.join(temp_dir, "job.tmp")
            with open(dst, "wb") as f:
                f.write(zlib.compress(response.serialized_job_data.encode("utf-8")))

            with open(dst, "rb") as f:
                resp = self._instance.requests_managed_retries_session.put(
                    self._instance.dagster_cloud_upload_job_snap_url,
                    headers=self._instance.headers_for_deployment(deployment_name),
                    data={},
                    files={"job.tmp": f},
                    timeout=self._instance.dagster_cloud_api_timeout,
                    proxies=self._instance.dagster_cloud_api_proxies,
                )
                raise_http_error(resp)
                self._logger.info(
                    "Successfully uploaded job snapshot for"
                    f" {job_selector.job_name}@{job_selector.repository_name} ({os.path.getsize(dst)} bytes)"
                )
                return response
