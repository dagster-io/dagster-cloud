import logging
import os
import sys
import tempfile
import threading
import time
import zlib
from abc import abstractmethod
from contextlib import AbstractContextManager
from typing import Collection, Dict, Generic, List, NamedTuple, Optional, Set, Tuple, TypeVar, Union

import dagster._check as check
from dagster import BoolSource, Field, IntSource
from dagster.api.get_server_id import sync_get_server_id
from dagster.api.list_repositories import sync_list_repositories_grpc
from dagster.core.errors import DagsterUserCodeUnreachableError
from dagster.core.host_representation import ExternalRepositoryOrigin
from dagster.core.host_representation.grpc_server_registry import GrpcServerEndpoint
from dagster.core.host_representation.origin import (
    RegisteredRepositoryLocationOrigin,
    RepositoryLocationOrigin,
)
from dagster.core.instance import MayHaveInstanceWeakref
from dagster.core.launcher import RunLauncher
from dagster.grpc.client import DagsterGrpcClient
from dagster.grpc.types import GetCurrentImageResult
from dagster.serdes import deserialize_as, serialize_dagster_namedtuple, whitelist_for_serdes
from dagster.utils import merge_dicts
from dagster.utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster_cloud.api.dagster_cloud_api import (
    DagsterCloudUploadLocationData,
    DagsterCloudUploadRepositoryData,
    DagsterCloudUploadWorkspaceEntry,
)
from dagster_cloud.execution.monitoring import (
    CloudRunWorkerStatuses,
    start_run_worker_monitoring_thread,
)
from dagster_cloud.instance import DagsterCloudAgentInstance
from dagster_cloud.util import diff_serializable_namedtuple_map
from dagster_cloud_cli.core.errors import raise_http_error
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata

DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT = 60


USER_CODE_LAUNCHER_RECONCILE_INTERVAL = 1

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
            "enabled": Field(
                BoolSource,
                is_required=False,
                default_value=False,
                description="Whether to shut down servers created by the agent when they are not serving requests",
            ),
            "ttl_seconds": Field(
                IntSource,
                is_required=False,
                default_value=3600,
                description="If enabled, how long to a leave server running once it has been launched. "
                "Decreasing this value will cause fewer servers to be running at once, but "
                "request latency may increase if more requests need to wait for a server to launch",
            ),
        },
        is_required=False,
    )
}

DeploymentAndLocation = Tuple[str, str]


class DagsterCloudUserCodeLauncher(
    AbstractContextManager, MayHaveInstanceWeakref[DagsterCloudAgentInstance], Generic[ServerHandle]
):
    def __init__(self, server_ttl):
        self._grpc_endpoints: Dict[
            DeploymentAndLocation, Union[GrpcServerEndpoint, SerializableErrorInfo]
        ] = {}
        self._grpc_endpoints_lock = threading.Lock()

        self._server_ttl_config = check.opt_dict_param(server_ttl, "server_ttl")

        # periodically reconciles to make desired = actual
        self._desired_entries: Dict[DeploymentAndLocation, UserCodeLauncherEntry] = {}
        self._actual_entries = {}
        self._metadata_lock = threading.Lock()

        self._upload_locations: Set[DeploymentAndLocation] = set()

        self._logger = logging.getLogger("dagster_cloud")
        self._started: bool = False
        self._run_worker_monitoring_thread = None
        self._run_worker_monitoring_thread_shutdown_event = None
        self._run_worker_statuses_list = []
        self._run_worker_statuses_list_lock = threading.Lock()

        self._reconcile_count = 0
        self._reconcile_grpc_metadata_shutdown_event = threading.Event()
        self._reconcile_grpc_metadata_thread = None

        super().__init__()

    @property
    def server_ttl_enabled(self) -> bool:
        return self._server_ttl_config.get("enabled", False)

    @property
    def server_ttl_seconds(self) -> int:
        return self._server_ttl_config.get("ttl_seconds", 3600)

    def start(self, run_reconcile_thread=True):
        # Initialize
        check.invariant(
            not self._started,
            "Called start() on a DagsterCloudUserCodeLauncher that was already started",
        )
        # Begin spinning user code up and down
        self._started = True

        if self._instance.run_launcher.supports_check_run_worker_health:
            self._logger.debug("Starting run worker monitoring.")
            (
                self._run_worker_monitoring_thread,
                self._run_worker_monitoring_thread_shutdown_event,
            ) = start_run_worker_monitoring_thread(
                self._instance, self._run_worker_statuses_list, self._run_worker_statuses_list_lock
            )
        else:
            self._logger.debug(
                "Not starting run worker monitoring, because it's not supported on this agent."
            )

        self._cleanup_servers()

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

    def get_cloud_run_worker_statuses(self):
        if not self._instance.run_launcher.supports_check_run_worker_health:
            return CloudRunWorkerStatuses(
                None, run_worker_monitoring_supported=False, run_worker_monitoring_thread_alive=None
            )

        self._logger.debug("Getting cloud run worker statuses for a heartbeat")

        with self._run_worker_statuses_list_lock:
            # values are immutable, don't need deepcopy
            statuses_list = self._run_worker_statuses_list.copy()
        self._logger.debug("Returning statuses_list: {}".format(statuses_list))
        return CloudRunWorkerStatuses(
            statuses=statuses_list,
            run_worker_monitoring_supported=True,
            run_worker_monitoring_thread_alive=self.is_run_worker_monitoring_thread_alive(),
        )

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
                )
                raise_http_error(resp)

                self._logger.info(
                    "Successfully uploaded workspace entry for {deployment_name}:{location_name}".format(
                        deployment_name=deployment_name, location_name=workspace_entry.location_name
                    )
                )

    def _get_upload_location_data(
        self, deployment_name: str, location_name: str
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
                    )
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
            "Unable to load {deployment_name}:{location_name}. Updating location with error data: {error_info}.".format(
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
        endpoint,
        metadata: CodeDeploymentMetadata,
    ):
        self._logger.info(
            "Uploading metadata for {deployment_name}:{location_name}".format(
                deployment_name=deployment_name, location_name=location_name
            )
        )

        try:
            if isinstance(endpoint, SerializableErrorInfo):
                self._update_location_error(
                    deployment_name, location_name, error_info=endpoint, metadata=metadata
                )
                return

            try:
                loaded_workspace_entry = DagsterCloudUploadWorkspaceEntry(
                    location_name=location_name,
                    deployment_metadata=metadata,
                    upload_location_data=self._get_upload_location_data(
                        deployment_name, location_name
                    ),
                    serialized_error_info=None,
                )
            except Exception:
                self._update_location_error(
                    deployment_name,
                    location_name,
                    error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                    metadata=metadata,
                )
                return

            self._logger.info(
                "Updating {deployment_name}:{location_name} with repository load data".format(
                    deployment_name=deployment_name,
                    location_name=location_name,
                )
            )
            self._update_workspace_entry(deployment_name, loaded_workspace_entry)
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

    @abstractmethod
    def _get_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[ServerHandle]:
        """Return a list of 'handles' that represent all running servers for a given location.
        Typically this will be a single server (unless an error was previous raised during a
        reconciliation loop. ServerHandle can be any type that is sufficient for
        _remove_server_handle to remove the service."""

    @abstractmethod
    def _create_new_server_endpoint(
        self,
        deployment_name: str,
        location_name: str,
        metadata: CodeDeploymentMetadata,
    ) -> GrpcServerEndpoint:
        """Create a new server for the given location using the given metadata as configuration
        and return a GrpcServerEndpoint indicating a hostname/port that can be used to access
        the server. Should result in an additional handle being returned from _get_server_handles_for_location."""

    @abstractmethod
    def _remove_server_handle(self, server_handle: ServerHandle) -> None:
        """Shut down any resources associated with the given handle. Called both during updates
        to spin down the old server once a new server has been spun up, and during removal."""

    @abstractmethod
    def _cleanup_servers(self):
        """Remove all servers, across all deployments and locations."""

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
            self._cleanup_servers()

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

    def _reconcile_thread(self, shutdown_event):
        while True:
            shutdown_event.wait(USER_CODE_LAUNCHER_RECONCILE_INTERVAL)
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

        self._reconcile(desired_entries, upload_locations)
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

    def _reconcile(
        self,
        desired_entries: Dict[DeploymentAndLocation, UserCodeLauncherEntry],
        upload_locations: Set[DeploymentAndLocation],
    ):
        diff = diff_serializable_namedtuple_map(desired_entries, self._actual_entries)

        to_update_keys = diff.to_add.union(diff.to_update)
        existing_server_handles: Dict[DeploymentAndLocation, Collection[ServerHandle]] = {}
        for to_update_key in to_update_keys:
            deployment_name, location_name = to_update_key

            try:
                code_deployment_metadata = desired_entries[to_update_key].code_deployment_metadata

                self._logger.info(
                    "Updating server for {deployment_name}:{location_name}".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                    )
                )
                existing_server_handles[to_update_key] = self._get_server_handles_for_location(
                    deployment_name, location_name
                )

                self._check_for_image(code_deployment_metadata)

                new_updated_endpoint: Union[
                    GrpcServerEndpoint, SerializableErrorInfo
                ] = self._create_new_server_endpoint(
                    deployment_name,
                    location_name,
                    code_deployment_metadata,
                )

                self._logger.info(
                    "Created a new server for location {location_name}".format(
                        location_name=to_update_key
                    )
                )
            except Exception:
                deployment_name, location_name = to_update_key
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error(
                    "Error while updating server for {deployment_name}:{location_name}: {error_info}".format(
                        deployment_name=deployment_name,
                        location_name=location_name,
                        error_info=error_info,
                    )
                )
                new_updated_endpoint = error_info

            with self._grpc_endpoints_lock:
                self._grpc_endpoints[to_update_key] = new_updated_endpoint

            if to_update_key in upload_locations:
                deployment_name, location_name = to_update_key
                upload_locations.remove(to_update_key)
                self._update_location_data(
                    deployment_name,
                    location_name,
                    new_updated_endpoint,
                    desired_entries[to_update_key].code_deployment_metadata,
                )

        for to_update_key in to_update_keys:
            deployment_name, location_name = to_update_key
            server_handles = existing_server_handles.get(to_update_key, [])

            if server_handles:
                self._logger.info(
                    "Removing {num_servers} existing servers for {deployment_name}:{location_name}".format(
                        num_servers=len(server_handles),
                        location_name=location_name,
                        deployment_name=deployment_name,
                    )
                )

            for server_handle in server_handles:
                try:
                    self._remove_server_handle(server_handle)
                except Exception:
                    self._logger.error(
                        "Error while cleaning up after updating server for {deployment_name}:{location_name}: {error_info}".format(
                            deployment_name=deployment_name,
                            location_name=location_name,
                            error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                        )
                    )

            if server_handles:
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

            with self._grpc_endpoints_lock:
                del self._grpc_endpoints[to_remove_key]
            del self._actual_entries[to_remove_key]

        # Upload any locations that were requested to be uploaded, but weren't updated
        # as part of this reconciliation loop
        for location in upload_locations:
            with self._grpc_endpoints_lock:
                endpoint = self._grpc_endpoints.get(location)

            deployment_name, location_name = location
            self._update_location_data(
                deployment_name,
                location_name,
                endpoint,
                self._actual_entries[location].code_deployment_metadata,
            )

    def has_grpc_endpoint(self, deployment_name: str, location_name: str) -> bool:
        with self._grpc_endpoints_lock:
            return (deployment_name, location_name) in self._grpc_endpoints

    def get_grpc_endpoint(
        self,
        deployment_name: str,
        location_name: str,
    ) -> GrpcServerEndpoint:
        with self._grpc_endpoints_lock:
            endpoint = self._grpc_endpoints.get((deployment_name, location_name))

        if not endpoint:
            raise DagsterUserCodeUnreachableError(
                f"No server endpoint exists for {deployment_name}:{location_name}"
            )

        if isinstance(endpoint, SerializableErrorInfo):
            # Consider raising the original exception here instead of a wrapped one
            raise DagsterUserCodeUnreachableError(
                f"Failure loading server endpoint for {deployment_name}:{location_name}: {endpoint}"
            )

        return endpoint

    def get_grpc_endpoints(
        self,
    ) -> Dict[DeploymentAndLocation, Union[GrpcServerEndpoint, SerializableErrorInfo]]:
        with self._grpc_endpoints_lock:
            return self._grpc_endpoints.copy()

    def _remove_server(self, deployment_name: str, location_name: str):
        self._logger.info(
            "Removing server for {deployment_name}:{location_name}".format(
                deployment_name=deployment_name, location_name=location_name
            )
        )
        existing_server_handles = self._get_server_handles_for_location(
            deployment_name, location_name
        )
        for server_handle in existing_server_handles:
            self._remove_server_handle(server_handle)

    def _wait_for_server(self, host: str, port: int, timeout, socket: Optional[str] = None) -> str:
        # Wait for the server to be ready (while also loading the server ID)
        server_id = None
        start_time = time.time()

        last_error = None

        while True:
            client = DagsterGrpcClient(port=port, host=host, socket=socket)
            try:
                server_id = sync_get_server_id(client)
                break
            except Exception:
                last_error = serializable_error_info_from_exc_info(sys.exc_info())

            if time.time() - start_time > timeout:
                raise Exception(
                    f"Timed out waiting for server {host}:{port}. Most recent connection error: {str(last_error)}"
                )

            time.sleep(1)
        return server_id
