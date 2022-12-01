import logging
import os
import sys
import tempfile
import threading
import time
import zlib
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import AbstractContextManager
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Generic,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import dagster._check as check
from dagster import BoolSource, Field, IntSource
from dagster._api.list_repositories import sync_list_repositories_grpc
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.host_representation import ExternalRepositoryOrigin, JobSelector
from dagster._core.host_representation.origin import (
    RegisteredRepositoryLocationOrigin,
    RepositoryLocationOrigin,
)
from dagster._core.instance import MayHaveInstanceWeakref
from dagster._core.launcher import RunLauncher
from dagster._grpc.client import DagsterGrpcClient
from dagster._grpc.types import GetCurrentImageResult, GetCurrentRunsResult
from dagster._serdes import deserialize_as, serialize_dagster_namedtuple, whitelist_for_serdes
from dagster._utils import merge_dicts
from dagster._utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster_cloud.api.dagster_cloud_api import (
    DagsterCloudUploadLocationData,
    DagsterCloudUploadRepositoryData,
    DagsterCloudUploadWorkspaceEntry,
    DagsterCloudUploadWorkspaceResponse,
)
from dagster_cloud.execution.monitoring import (
    CloudCodeServerHeartbeat,
    CloudCodeServerStatus,
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
from dagster_cloud_cli.core.errors import raise_http_error
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata

DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT = 180
DEFAULT_MAX_TTL_SERVERS = 25


USER_CODE_LAUNCHER_RECONCILE_SLEEP_SECONDS = 1

# Check on pending delete servers every 30th reconcile
PENDING_DELETE_SERVER_CHECK_INTERVAL = 30

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
            ("sandbox_saved_timestamp", Optional[float]),
        ],
    )
):
    def __new__(
        cls,
        code_deployment_metadata,
        update_timestamp,
        sandbox_saved_timestamp=None,
    ):
        return super(UserCodeLauncherEntry, cls).__new__(
            cls,
            check.inst_param(
                code_deployment_metadata, "code_deployment_metadata", CodeDeploymentMetadata
            ),
            check.float_param(update_timestamp, "update_timestamp"),
            check.opt_float_param(sandbox_saved_timestamp, "sandbox_saved_timestamp"),
        )


SHARED_USER_CODE_LAUNCHER_CONFIG = {
    "server_ttl": Field(
        {
            "full_deployments": Field(
                {
                    "enabled": Field(
                        BoolSource,
                        is_required=True,
                        description="Whether to shut down servers created by the agent for full deployments when they are not serving requests",
                    ),
                    "ttl_seconds": Field(
                        IntSource,
                        is_required=False,
                        default_value=DEFAULT_SERVER_TTL_SECONDS,
                        description="If the `enabled` flag is set , how long to leave a server "
                        "running for a once it has been launched. Decreasing this value will cause "
                        "fewer servers to be running at once, but request latency may increase "
                        "if more requests need to wait for a server to launch",
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
                        description="How long to leave a server for a branch deployment running "
                        "once it has been launched. Decreasing this value will cause fewer servers "
                        "to be running at once, but request latency may increase if more requests "
                        "need to wait for a server to launch",
                    ),
                },
                is_required=False,
            ),
            "max_servers": Field(
                IntSource,
                is_required=False,
                default_value=DEFAULT_MAX_TTL_SERVERS,
                description="In addition to the TTL, ensure that the maximum number of "
                "servers that are up at any given time and not currently serving requests stays "
                "below this number.",
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
        description="Do not include full job snapshots in the workspace "
        "snapshot, upload them separately if they have not been previously uploaded.",
    ),
}

DeploymentAndLocation = Tuple[str, str]


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
    ):
        self._grpc_servers: Dict[
            DeploymentAndLocation, Union[DagsterCloudGrpcServer, SerializableErrorInfo]
        ] = {}
        self._pending_delete_grpc_server_handles: Set[ServerHandle] = set()
        self._grpc_servers_lock = threading.Lock()

        self._multipex_servers: Dict[DeploymentAndLocation, DagsterCloudGrpcServer] = {}

        self._server_ttl_config = check.opt_dict_param(server_ttl, "server_ttl")
        self._defer_job_snapshots = defer_job_snapshots

        # periodically reconciles to make desired = actual
        self._desired_entries: Dict[DeploymentAndLocation, UserCodeLauncherEntry] = {}
        self._actual_entries: Dict[DeploymentAndLocation, UserCodeLauncherEntry] = {}
        self._metadata_lock = threading.Lock()

        self._upload_locations: Set[DeploymentAndLocation] = set()

        self._logger = logging.getLogger("dagster_cloud.user_code_launcher")
        self._started: bool = False
        self._run_worker_monitoring_thread = None
        self._run_worker_monitoring_thread_shutdown_event = None
        self._run_worker_deployments_to_check: Set[str] = set()
        self._run_worker_statuses_dict: Dict[str, List[CloudRunWorkerStatus]] = {}
        self._run_worker_monitoring_lock = threading.Lock()

        self._reconcile_count = 0
        self._reconcile_grpc_metadata_shutdown_event = threading.Event()
        self._reconcile_grpc_metadata_thread = None

        self._server_process_startup_timeout = check.opt_int_param(
            server_process_startup_timeout,
            "server_process_startup_timeout",
            DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
        )

        super().__init__()

    def get_active_grpc_server_handles(self) -> List[ServerHandle]:
        with self._grpc_servers_lock:
            return [
                s.server_handle
                for s in self._grpc_servers.values()
                if not isinstance(s, SerializableErrorInfo)
            ] + list(self._pending_delete_grpc_server_handles)

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

    @property
    def server_ttl_max_servers(self) -> int:
        return self._server_ttl_config.get("max_servers", DEFAULT_MAX_TTL_SERVERS)

    def start(self, run_reconcile_thread=True):
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
            (
                self._run_worker_monitoring_thread,
                self._run_worker_monitoring_thread_shutdown_event,
            ) = start_run_worker_monitoring_thread(
                self._instance,
                self._run_worker_deployments_to_check,
                self._run_worker_statuses_dict,
                self._run_worker_monitoring_lock,
            )
        else:
            self._logger.debug(
                "Not starting run worker monitoring, because it's not supported on this agent."
            )

        self._graceful_cleanup_servers()

        if run_reconcile_thread:
            self._reconcile_grpc_metadata_thread = threading.Thread(
                target=self._reconcile_thread,
                args=(self._reconcile_grpc_metadata_shutdown_event,),
                name="grpc-reconcile-watch",
            )
            self._reconcile_grpc_metadata_thread.daemon = True
            self._reconcile_grpc_metadata_thread.start()

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
        self._logger.debug("Returning statuses_dict: {}".format(statuses_dict))

        is_alive = self.is_run_worker_monitoring_thread_alive()

        return {
            deployment_name: CloudRunWorkerStatuses(
                statuses=statuses_dict.get(deployment_name, []),
                run_worker_monitoring_supported=True,
                run_worker_monitoring_thread_alive=is_alive,
            )
            for deployment_name in deployment_names
        }

    def supports_origin(self, repository_location_origin: RepositoryLocationOrigin) -> bool:
        return isinstance(repository_location_origin, RegisteredRepositoryLocationOrigin)

    @property
    def supports_reload(self) -> bool:
        return False

    def _update_workspace_entry(
        self, deployment_name: str, workspace_entry: DagsterCloudUploadWorkspaceEntry
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            dst = os.path.join(temp_dir, "workspace_entry.tmp")
            with open(dst, "wb") as f:
                f.write(
                    zlib.compress(serialize_dagster_namedtuple(workspace_entry).encode("utf-8"))
                )

            with open(dst, "rb") as f:
                self._logger.info(
                    "Uploading workspace entry for {deployment_name}:{location_name} ({size} bytes)".format(
                        deployment_name=deployment_name,
                        location_name=workspace_entry.location_name,
                        size=os.path.getsize(dst),
                    )
                )

                resp = self._instance.requests_session.post(
                    self._instance.dagster_cloud_upload_workspace_entry_url,
                    headers=self._instance.headers_for_deployment(deployment_name),
                    data={},
                    files={"workspace_entry.tmp": f},
                    timeout=self._instance.dagster_cloud_api_timeout,
                    proxies=self._instance.dagster_cloud_api_proxies,
                )
                raise_http_error(resp)

            response = deserialize_as(resp.text, DagsterCloudUploadWorkspaceResponse)
            self._logger.info(
                f"Workspace entry for {deployment_name}:{workspace_entry.location_name} {response.message}"
            )

            # if the update took we are all done
            if response.updated:
                return

            # if not there must be missing job snapshots, upload them and then try again
            missing = response.missing_job_snapshots
            if missing is None:
                check.failed(
                    "Unexpected state: workspace was not updated but no required job snapshots were returned."
                )

            self._logger.info(f"Uploading {len(missing)} job snapshots.")
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(self.upload_job_snapshot, deployment_name, job_selector)
                    for job_selector in missing
                ]
                wait(futures)
                # trigger any exceptions to throw
                _ = [f.result() for f in futures]

            with open(dst, "rb") as f:
                resp = self._instance.requests_session.post(
                    self._instance.dagster_cloud_upload_workspace_entry_url,
                    headers=self._instance.headers_for_deployment(deployment_name),
                    data={},
                    files={"workspace_entry.tmp": f},
                    timeout=self._instance.dagster_cloud_api_timeout,
                    proxies=self._instance.dagster_cloud_api_proxies,
                )
                raise_http_error(resp)

            response = deserialize_as(resp.text, DagsterCloudUploadWorkspaceResponse)
            check.invariant(response.updated, "update failed after job snapshots uploaded")
            self._logger.info(
                f"Workspace entry for {deployment_name}:{workspace_entry.location_name} {response.message}"
            )

    def _get_upload_location_data(
        self,
        deployment_name: str,
        location_name: str,
    ) -> DagsterCloudUploadLocationData:
        location_origin = self._get_repository_location_origin(location_name)
        client = self.get_grpc_endpoint(deployment_name, location_name).create_client()

        list_repositories_response = sync_list_repositories_grpc(client)

        upload_repo_datas: List[DagsterCloudUploadRepositoryData] = []

        for (
            repository_name,
            code_pointer,
        ) in list_repositories_response.repository_code_pointer_dict.items():
            external_repository_chunks = list(
                client.streaming_external_repository(
                    external_repository_origin=ExternalRepositoryOrigin(
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
            container_image=deserialize_as(
                client.get_current_image(), GetCurrentImageResult
            ).current_image,
            executable_path=list_repositories_response.executable_path,
        )

    def _update_location_error(
        self,
        deployment_name: str,
        location_name: str,
        error_info: SerializableErrorInfo,
        metadata: CodeDeploymentMetadata,
    ):
        self._logger.error(
            "Unable to update {deployment_name}:{location_name}. Updating location with error data: {error_info}.".format(
                deployment_name=deployment_name,
                location_name=location_name,
                error_info=str(error_info),
            )
        )

        # Update serialized error
        errored_workspace_entry = DagsterCloudUploadWorkspaceEntry(
            location_name=location_name,
            deployment_metadata=metadata,
            upload_location_data=None,
            serialized_error_info=error_info,
        )

        self._update_workspace_entry(deployment_name, errored_workspace_entry)

    def _update_location_data(
        self,
        deployment_name: str,
        location_name: str,
        server_or_error: Union[DagsterCloudGrpcServer, SerializableErrorInfo],
        metadata: CodeDeploymentMetadata,
    ):
        self._logger.info(
            "Fetching metadata for {deployment_name}:{location_name}".format(
                deployment_name=deployment_name, location_name=location_name
            )
        )

        try:
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
                    ),
                    serialized_error_info=None,
                )

                self._logger.info(
                    "Updating {deployment_name}:{location_name} with repository load data".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                    )
                )

                self._update_workspace_entry(deployment_name, loaded_workspace_entry)
            except Exception:
                # Try to write the error to cloud. If this doesn't work the outer try block
                # prevents this from blocking the agent loop.
                self._update_location_error(
                    deployment_name,
                    location_name,
                    error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                    metadata=metadata,
                )
                return

        except Exception:
            # Don't let a failure uploading the serialized data keep other locations
            # from being updated or continue reconciling in a loop
            error_info = serializable_error_info_from_exc_info(sys.exc_info())
            self._logger.error(
                "Error while writing location data for {deployment_name}:{location_name}: {error_info}".format(
                    deployment_name=deployment_name,
                    location_name=location_name,
                    error_info=error_info,
                )
            )

    @property
    @abstractmethod
    def requires_images(self) -> bool:
        pass

    def _get_existing_pex_servers(
        self, deployment_name: str, location_name: str
    ) -> List[PexServerHandle]:

        server = self._multipex_servers.get((deployment_name, location_name))

        if not server:
            return []

        _server_handle, server_endpoint, _code_deployment_metadata = server
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

    @abstractmethod
    def _get_standalone_dagster_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[ServerHandle]:
        """Return a list of 'handles' that represent all running servers for a given location
        that are running the dagster grpc server as the entry point (i.e. are not multipex
        servers). Typically this will be a single server (unless an error was previous raised
        during a reconciliation loop. ServerHandle can be any type that is sufficient to uniquely
        identify the server and can be passed into _remove_server_handle to remove the server."""

    def _get_multipex_server_handles_for_location(
        self, _deployment_name: str, _location_name: str
    ) -> Collection[ServerHandle]:
        """Return a list of 'handles' that represent all servers running the multipex server
        entrypoint."""
        return []

    @abstractmethod
    def _start_new_server_spinup(
        self,
        deployment_name: str,
        location_name: str,
        metadata: CodeDeploymentMetadata,
    ) -> DagsterCloudGrpcServer:
        """Create a new server for the given location using the given metadata as configuration
        and return a ServerHandle indicating where it can be found. Any waiting for the server
        to happen should happen in _wait_for_new_server_endpoint."""

    def _wait_for_new_multipex_server(
        self, _deployment_name: str, _location_name: str, multipex_endpoint: ServerEndpoint
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
        metadata: CodeDeploymentMetadata,
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
        to spin down the old server once a new server has been spun up, and during removal."""

    @property
    def _supports_graceful_remove(self) -> bool:
        return False

    def _get_grpc_client(self, server_handle: ServerHandle) -> DagsterGrpcClient:
        raise NotImplementedError()

    def _graceful_remove_server_handle(self, server_handle: ServerHandle):
        """Check if there are non isolated runs and wait for them to finish before shutting down
        the server."""

        if not self._supports_graceful_remove:
            return self._remove_server_handle(server_handle)

        run_ids = None
        try:
            client = self._get_grpc_client(server_handle)
            run_ids = deserialize_as(client.get_current_runs(), GetCurrentRunsResult).current_runs
        except Exception:
            self._logger.error(
                "Failure connecting to server with handle {server_handle}, going to shut it down: {exc_info}".format(
                    server_handle=server_handle,
                    exc_info=serializable_error_info_from_exc_info(sys.exc_info()),
                )
            )

        if run_ids:
            self._logger.info(
                f"Waiting for run_ids [{', '.join(run_ids)}] to finish before shutting down server {server_handle}"
            )
            with self._grpc_servers_lock:
                self._pending_delete_grpc_server_handles.add(server_handle)
        else:
            if run_ids == []:  # If it's None, the grpc call failed
                self._logger.info(f"No runs, shutting down server {server_handle}")
            self._remove_server_handle(server_handle)
            with self._grpc_servers_lock:
                self._pending_delete_grpc_server_handles.discard(server_handle)

    @abstractmethod
    def _cleanup_servers(self):
        """Remove all servers, across all deployments and locations."""

    def _list_server_handles(self) -> List[ServerHandle]:
        """Return a list of all server handles across all deployments and locations."""
        raise NotImplementedError()

    def _graceful_cleanup_servers(self):  # ServerHandles
        if not self._supports_graceful_remove:
            return self._cleanup_servers()

        handles = self._list_server_handles()
        with self._grpc_servers_lock:
            self._pending_delete_grpc_server_handles.update(handles)
        for server_handle in handles:
            self._graceful_remove_server_handle(server_handle)

    @abstractmethod
    def run_launcher(self) -> RunLauncher:
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        if self._reconcile_grpc_metadata_thread:
            self._reconcile_grpc_metadata_shutdown_event.set()
            self._reconcile_grpc_metadata_thread.join()

        if self._run_worker_monitoring_thread:
            self._run_worker_monitoring_thread_shutdown_event.set()
            self._run_worker_monitoring_thread.join()

        if self._started:
            self._graceful_cleanup_servers()

        super().__exit__(exception_value, exception_value, traceback)

    def add_upload_metadata(self, upload_metadata: Dict[Tuple[str, str], UserCodeLauncherEntry]):
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

    def _get_repository_location_origin(
        self, location_name: str
    ) -> RegisteredRepositoryLocationOrigin:
        return RegisteredRepositoryLocationOrigin(location_name)

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
                    "Failure updating user code servers: {exc_info}".format(
                        exc_info=serializable_error_info_from_exc_info(sys.exc_info()),
                    )
                )

    def reconcile(self):
        with self._metadata_lock:
            desired_entries = (
                self._desired_entries.copy() if self._desired_entries != None else None
            )
            upload_locations = self._upload_locations.copy()
            self._upload_locations = set()

        if desired_entries == None:
            # Wait for the first time the desired metadata is set before reconciling
            return

        self._reconcile(
            desired_entries,
            upload_locations,
            check_on_pending_delete_servers=self._reconcile_count % self._reconcile_interval == 0,
        )
        self._reconcile_count += 1

    def _check_for_image(self, metadata: CodeDeploymentMetadata):
        if self.requires_images and not metadata.image:
            raise Exception(
                "Your agent's configuration requires you to specify an image. "
                "Use the `--image` flag when specifying your location to tell the agent "
                "which image to use to load your code."
            )

        if (not self.requires_images) and metadata.image:
            raise Exception(
                "Your agent's configuration cannot load locations that specify a Docker "
                "image. Either update your location to not include an image, or change the `user_code_launcher` "
                "field in your agent's `dagster.yaml` file to a launcher that can load Docker images. "
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
            f"Reconciling to reach {goal_str}. To add: {to_add_str}. To update: {to_update_str}. To remove: {to_remove_str}. To upload: {to_upload_str}."
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
            existing_multipex_server_handles[
                to_update_key
            ] = self._get_multipex_server_handles_for_location(deployment_name, location_name)

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
                                "Spinning up a new multipex server for {deployment_name}:{location_name} since the existing one failed with the following error: {error_info}".format(
                                    deployment_name=deployment_name,
                                    location_name=location_name,
                                    error_info=error_info,
                                )
                            )
                            multipex_server = None

                    if not multipex_server:
                        self._logger.info(
                            "Creating new multipex server for {deployment_name}:{location_name}".format(
                                deployment_name=deployment_name,
                                location_name=location_name,
                            )
                        )
                        multipex_server = self._create_multipex_server(
                            deployment_name, location_name, desired_entry.code_deployment_metadata
                        )
                        assert self._get_multipex_server(
                            deployment_name,
                            location_name,
                            desired_entry.code_deployment_metadata,
                        )
                        new_multipex_servers[to_update_key] = multipex_server
                    else:
                        self._logger.info(
                            "Found running multipex server for {deployment_name}:{location_name}".format(
                                deployment_name=deployment_name,
                                location_name=location_name,
                            )
                        )

                except Exception:
                    error_info = serializable_error_info_from_exc_info(sys.exc_info())
                    self._logger.error(
                        "Error while setting up multipex server for {deployment_name}:{location_name}: {error_info}".format(
                            deployment_name=deployment_name,
                            location_name=location_name,
                            error_info=error_info,
                        )
                    )
                    new_dagster_servers[to_update_key] = error_info

        # For each new multi-pex server, wait for it to be ready. If it fails, put
        # the location that was planned to use it into an error state
        for to_update_key, multipex_server in new_multipex_servers.items():
            deployment_name, location_name = to_update_key

            try:
                self._logger.info(
                    "Waiting for new multipex server for {deployment_name}:{location_name} to be ready".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                    )
                )
                self._wait_for_new_multipex_server(
                    deployment_name, location_name, multipex_server.server_endpoint
                )
            except Exception:
                error_info = serializable_error_info_from_exc_info(sys.exc_info())

                self._logger.error(
                    "Error while waiting for multipex server for {deployment_name}:{location_name}: {error_info}".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                        error_info=error_info,
                    )
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

                self._logger.info(
                    "Updating server for {deployment_name}:{location_name}".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                    )
                )
                existing_standalone_dagster_server_handles[
                    to_update_key
                ] = self._get_standalone_dagster_server_handles_for_location(
                    deployment_name, location_name
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

                self._logger.info(
                    "Created a new server for {location_name}".format(location_name=to_update_key)
                )
            except Exception:
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error(
                    "Error while updating server for {deployment_name}:{location_name}: {error_info}".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                        error_info=error_info,
                    )
                )
                new_dagster_servers[to_update_key] = error_info

        # Wait for all new dagster servers (standalone or within a multipex server) to be ready
        for to_update_key in to_update_keys:
            deployment_name, location_name = to_update_key
            code_deployment_metadata = desired_entries[to_update_key].code_deployment_metadata
            server_or_error = new_dagster_servers[to_update_key]

            if not isinstance(server_or_error, SerializableErrorInfo):
                try:
                    self._logger.info(
                        "Waiting for new grpc server for {location_name} to be ready...".format(
                            location_name=to_update_key
                        )
                    )
                    self._wait_for_new_server_ready(
                        deployment_name,
                        location_name,
                        code_deployment_metadata,
                        server_or_error.server_handle,
                        server_or_error.server_endpoint,
                    )
                except:
                    error_info = serializable_error_info_from_exc_info(sys.exc_info())
                    self._logger.error(
                        "Error while waiting for server for {deployment_name}:{location_name} to be ready: {error_info}".format(
                            deployment_name=deployment_name,
                            location_name=location_name,
                            error_info=error_info,
                        )
                    )
                    server_or_error = error_info

            with self._grpc_servers_lock:
                self._grpc_servers[to_update_key] = server_or_error

            if to_update_key in upload_locations:
                upload_locations.remove(to_update_key)
                self._update_location_data(
                    deployment_name,
                    location_name,
                    server_or_error,
                    desired_entries[to_update_key].code_deployment_metadata,
                )

        # Remove any old standalone grpc server containers
        for to_update_key in to_update_keys:
            deployment_name, location_name = to_update_key
            server_handles = existing_standalone_dagster_server_handles.get(to_update_key, [])
            removed_any_servers = False

            if server_handles:
                removed_any_servers = True
                self._logger.info(
                    "Removing {num_servers} existing servers for {deployment_name}:{location_name}".format(
                        num_servers=len(server_handles),
                        location_name=location_name,
                        deployment_name=deployment_name,
                    )
                )

            for server_handle in server_handles:
                try:
                    self._graceful_remove_server_handle(server_handle)
                except Exception:
                    self._logger.error(
                        "Error while cleaning up after updating server for {deployment_name}:{location_name}: {error_info}".format(
                            deployment_name=deployment_name,
                            location_name=location_name,
                            error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                        )
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
                    current_multipex_server_handle
                    and current_multipex_server_handle != multipex_server_handle
                ):
                    self._logger.info(
                        "Removing old multipex server for {deployment_name}:{location_name}".format(
                            location_name=location_name,
                            deployment_name=deployment_name,
                        )
                    )

                    try:
                        self._graceful_remove_server_handle(multipex_server_handle)
                    except Exception:
                        self._logger.error(
                            "Error while cleaning up old multipex server for {deployment_name}:{location_name}: {error_info}".format(
                                deployment_name=deployment_name,
                                location_name=location_name,
                                error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                            )
                        )

            # On the current multipex server, shut down any old pex servers
            pex_server_handles = existing_pex_server_handles.get(to_update_key)
            if pex_server_handles:
                assert current_multipex_server
                removed_any_servers = True
                self._logger.info(
                    "Removing {num_servers} grpc processes from multipex server for {deployment_name}:{location_name}".format(
                        num_servers=len(pex_server_handles),
                        location_name=location_name,
                        deployment_name=deployment_name,
                    )
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
                            "Error while cleaning up after updating server for {deployment_name}:{location_name}: {error_info}".format(
                                deployment_name=deployment_name,
                                location_name=location_name,
                                error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                            )
                        )

            if removed_any_servers:
                self._logger.info(
                    "Removed all previous servers for {deployment_name}:{location_name}".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                    )
                )

            # Always update our actual entries
            self._actual_entries[to_update_key] = desired_entries[to_update_key]

        for to_remove_key in diff.to_remove:
            deployment_name, location_name = to_remove_key
            try:
                self._remove_server(deployment_name, location_name)
            except Exception:
                self._logger.error(
                    "Error while removing server for {deployment_name}:{location_name}: {error_info}".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                        error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                    )
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
            self._update_location_data(
                deployment_name,
                location_name,
                server_or_error,
                self._actual_entries[location].code_deployment_metadata,
            )

        self._logger.info(f"Finished reconciling in {time.time() - start_time} seconds.")

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

        if (cand_server.code_deployment_metadata.image == code_deployment_metadata.image) and (
            cand_server.code_deployment_metadata.container_context
            == code_deployment_metadata.container_context
        ):
            return cand_server

        return None

    def _create_multipex_server(self, deployment_name, location_name, code_deployment_metadata):
        multipex_server = self._start_new_server_spinup(
            deployment_name,
            location_name,
            code_deployment_metadata,
        )
        self._multipex_servers[(deployment_name, location_name)] = multipex_server
        return multipex_server

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
            return self._start_new_server_spinup(
                deployment_name, location_name, desired_entry.code_deployment_metadata
            )

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
                    error=endpoint_or_error
                    if isinstance(endpoint_or_error, SerializableErrorInfo)
                    else None,
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
        self._logger.info(
            "Removing server for {deployment_name}:{location_name}".format(
                deployment_name=deployment_name, location_name=location_name
            )
        )
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
    ) -> None:
        self._wait_for_server_process(client, timeout, additional_check)
        # Call a method that raises an exception if there was an error importing the code
        sync_list_repositories_grpc(client)

    def _wait_for_server_process(
        self,
        client: Union[DagsterGrpcClient, MultiPexGrpcClient],
        timeout,
        additional_check: Optional[Callable[[], None]] = None,
    ) -> None:
        start_time = time.time()

        last_error = None

        while True:
            try:
                client.ping("")
                break
            except Exception:
                last_error = serializable_error_info_from_exc_info(sys.exc_info())

            if time.time() - start_time > timeout:
                raise Exception(
                    f"Timed out after waiting {timeout}s for server {client.host}:{client.port or client.socket}. "
                    f"Most recent connection error: {str(last_error)}"
                )

            time.sleep(1)

            if additional_check:
                additional_check()

    def upload_job_snapshot(
        self,
        deployment_name: str,
        job_selector: JobSelector,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = self.get_grpc_endpoint(
                deployment_name, job_selector.location_name
            ).create_client()
            location_origin = self._get_repository_location_origin(job_selector.location_name)
            response = client.external_job(
                ExternalRepositoryOrigin(location_origin, job_selector.repository_name),
                job_selector.job_name,
            )
            if not response.serialized_job_data:
                error = (
                    deserialize_as(response.serialized_error, SerializableErrorInfo)
                    if response.serialized_error
                    else "no captured error"
                )
                raise Exception(f"Error fetching job data in code server:\n{error}")

            dst = os.path.join(temp_dir, "job.tmp")
            with open(dst, "wb") as f:
                f.write(zlib.compress(response.serialized_job_data.encode("utf-8")))

            with open(dst, "rb") as f:
                resp = self._instance.requests_session.post(
                    self._instance.dagster_cloud_upload_job_snap_url,
                    headers=self._instance.headers_for_deployment(deployment_name),
                    data={},
                    files={"job.tmp": f},
                    timeout=self._instance.dagster_cloud_api_timeout,
                    proxies=self._instance.dagster_cloud_api_proxies,
                )
                raise_http_error(resp)
                self._logger.info(
                    f"Successfully uploaded job snapshot for {job_selector.job_name}@{job_selector.repository_name} ({os.path.getsize(dst)} bytes)"
                )
                return response
