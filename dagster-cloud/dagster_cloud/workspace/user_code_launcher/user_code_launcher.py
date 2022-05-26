import logging
import os
import sys
import tempfile
import threading
import time
import zlib
from abc import abstractmethod
from contextlib import AbstractContextManager
from typing import Collection, Dict, Generic, List, NamedTuple, Optional, Set, TypeVar, Union

import dagster._check as check
from dagster.api.get_server_id import sync_get_server_id
from dagster.api.list_repositories import sync_list_repositories_grpc
from dagster.core.errors import DagsterUserCodeUnreachableError
from dagster.core.executor.step_delegating import StepHandler
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
from dagster.utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster_cloud.api.dagster_cloud_api import (
    DagsterCloudSandboxConnectionInfo,
    DagsterCloudSandboxProxyInfo,
    DagsterCloudUploadLocationData,
    DagsterCloudUploadRepositoryData,
    DagsterCloudUploadWorkspaceEntry,
)
from dagster_cloud.errors import raise_http_error
from dagster_cloud.execution.monitoring import (
    CloudRunWorkerStatuses,
    start_run_worker_monitoring_thread,
)
from dagster_cloud.instance import DagsterCloudAgentInstance
from dagster_cloud.util import diff_serializable_namedtuple_map
from dagster_cloud.workspace.origin import CodeDeploymentMetadata

DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT = 60


USER_CODE_LAUNCHER_RECONCILE_INTERVAL = 1

ServerHandle = TypeVar("ServerHandle")

SANDBOX_QUERY = """
    query DeploymentInfo {
         deploymentInfo {
             deploymentType
         }
     }
"""

AUTHORIZED_KEY_QUERY = """
    query DeploymentInfo {
         deploymentInfo {
             sshKeys {
                publicKey
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
            ("sandbox_proxy_info", Optional[DagsterCloudSandboxProxyInfo]),
        ],
    )
):
    def __new__(
        cls,
        code_deployment_metadata,
        update_timestamp,
        sandbox_saved_timestamp=None,
        sandbox_proxy_info=None,
    ):
        return super(UserCodeLauncherEntry, cls).__new__(
            cls,
            check.inst_param(
                code_deployment_metadata, "code_deployment_metadata", CodeDeploymentMetadata
            ),
            check.float_param(update_timestamp, "update_timestamp"),
            check.opt_float_param(sandbox_saved_timestamp, "sandbox_saved_timestamp"),
            check.opt_inst_param(
                sandbox_proxy_info, "sandbox_proxy_info", DagsterCloudSandboxProxyInfo
            ),
        )


class DagsterCloudUserCodeLauncher(
    AbstractContextManager, MayHaveInstanceWeakref[DagsterCloudAgentInstance], Generic[ServerHandle]
):
    def __init__(self):
        self._grpc_endpoints: Dict[str, Union[GrpcServerEndpoint, SerializableErrorInfo]] = {}
        self._grpc_endpoints_lock = threading.Lock()

        # periodically reconciles to make desired = actual
        self._desired_entries: Optional[Dict[str, UserCodeLauncherEntry]] = None
        self._actual_entries = {}
        self._metadata_lock = threading.Lock()

        self._upload_locations: Set[str] = set()

        self._logger = logging.getLogger("dagster_cloud")
        self._started: bool = False
        self._run_worker_monitoring_thread = None
        self._run_worker_monitoring_thread_shutdown_event = None
        self._run_worker_statuses_list = []
        self._run_worker_statuses_list_lock = threading.Lock()

        self._reconcile_count = 0
        self._reconcile_grpc_metadata_shutdown_event = threading.Event()
        self._reconcile_grpc_metadata_thread = None

        self._is_workspace_ready_lock = threading.Lock()
        self._is_workspace_ready: bool = False

        self._is_dev_sandbox = None
        super().__init__()

    @property
    def supports_dev_sandbox(self) -> bool:
        return False

    def is_dev_sandbox(self):
        if self._is_dev_sandbox == None and self.supports_dev_sandbox:
            client = self._instance.graphql_client
            result = client.execute(SANDBOX_QUERY)
            self._is_dev_sandbox = result["data"]["deploymentInfo"]["deploymentType"] == "DEV"
        return self._is_dev_sandbox

    def _authorized_key(self) -> Optional[str]:
        if self.is_dev_sandbox():
            client = self._instance.graphql_client
            result = client.execute(AUTHORIZED_KEY_QUERY)
            keys = [key["publicKey"] for key in result["data"]["deploymentInfo"]["sshKeys"]]
            if keys:
                # TODO: Support multiple keys
                return keys[0]
        return None

    def start(self, run_reconcile_thread=True):
        # Initialize
        self.is_dev_sandbox()

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

    def _update_workspace_entry(self, workspace_entry: DagsterCloudUploadWorkspaceEntry):
        with tempfile.TemporaryDirectory() as temp_dir:
            dst = os.path.join(temp_dir, "workspace_entry.tmp")
            with open(dst, "wb") as f:
                f.write(
                    zlib.compress(serialize_dagster_namedtuple(workspace_entry).encode("utf-8"))
                )

            with open(dst, "rb") as f:
                self._logger.info(
                    "Uploading workspace entry for {location_name} ({size} bytes)".format(
                        location_name=workspace_entry.location_name, size=os.path.getsize(dst)
                    )
                )

                connection_info = None
                if self.is_dev_sandbox():
                    connection_info = self.get_sandbox_connection_info(
                        workspace_entry.location_name,
                    )
                resp = self._instance.requests_session.post(
                    self._instance.dagster_cloud_upload_workspace_entry_url,
                    headers=self._instance.dagster_cloud_api_headers,
                    data={
                        "sandbox_connection_info": serialize_dagster_namedtuple(connection_info)
                        if connection_info
                        else None
                    },
                    files={"workspace_entry.tmp": f},
                    timeout=self._instance.dagster_cloud_api_timeout,
                )
                raise_http_error(resp)

                self._logger.info(
                    "Successfully uploaded workspace entry for {location_name}".format(
                        location_name=workspace_entry.location_name
                    )
                )

    def _get_upload_location_data(self, location_name: str) -> DagsterCloudUploadLocationData:
        location_origin = self._get_repository_location_origin(location_name)
        client = self.get_grpc_endpoint(location_origin).create_client()

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
        location_name: str,
        error_info: SerializableErrorInfo,
        metadata: CodeDeploymentMetadata,
    ):
        self._logger.error(
            "Unable to load location {location_name}. Updating location with error data: {error_info}.".format(
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

        self._update_workspace_entry(errored_workspace_entry)

    def _update_location_data(self, location_name: str, endpoint, metadata: CodeDeploymentMetadata):
        self._logger.info(
            "Uploading metadata for location {location_name}".format(location_name=location_name)
        )

        try:
            if isinstance(endpoint, SerializableErrorInfo):
                self._update_location_error(location_name, error_info=endpoint, metadata=metadata)
                return

            try:
                loaded_workspace_entry = DagsterCloudUploadWorkspaceEntry(
                    location_name=location_name,
                    deployment_metadata=metadata,
                    upload_location_data=self._get_upload_location_data(location_name),
                    serialized_error_info=None,
                )
            except Exception:
                self._update_location_error(
                    location_name,
                    error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                    metadata=metadata,
                )
                return

            self._logger.info(
                "Updating location {location_name} with repository load data".format(
                    location_name=location_name,
                )
            )
            self._update_workspace_entry(loaded_workspace_entry)
        except Exception:
            # Don't let a failure uploading the serialized data keep other locations
            # from being updated or continue reconciling in a loop
            error_info = serializable_error_info_from_exc_info(sys.exc_info())
            self._logger.error(
                "Error while writing location data for location {location_name}: {error_info}".format(
                    location_name=location_name,
                    error_info=error_info,
                )
            )

    @property
    def is_workspace_ready(self) -> bool:
        with self._is_workspace_ready_lock:
            return self._is_workspace_ready

    @property
    @abstractmethod
    def requires_images(self) -> bool:
        pass

    @abstractmethod
    def _get_server_handles_for_location(self, location_name: str) -> Collection[ServerHandle]:
        """Return a list of 'handles' that represent all running servers for a given location.
        Typically this will be a single server (unless an error was previous raised during a
        reconciliation loop. ServerHandle can be any type that is sufficient for
        _remove_server_handle to remove the service."""

    @abstractmethod
    def _create_new_server_endpoint(
        self, location_name: str, metadata: CodeDeploymentMetadata
    ) -> GrpcServerEndpoint:
        """Create a new server for the given location using the given metadata as configuration
        and return a GrpcServerEndpoint indicating a hostname/port that can be used to access
        the server. Should result in an additional handle being returned from _get_server_handles_for_location."""

    @abstractmethod
    def _create_dev_sandbox_endpoint(
        self,
        location_name: str,
        metadata: CodeDeploymentMetadata,
        authorized_key: str,
        proxy_info: DagsterCloudSandboxProxyInfo,
    ) -> GrpcServerEndpoint:
        """Create a new dev sandbox for the given location using the given metadata as configuration
        and return a GrpcServerEndpoint indicating a hostname/port that can be used to access
        the server. Should result in an additional handle being returned from _get_server_handles_for_location."""

    @abstractmethod
    def get_sandbox_connection_info(self, location_name: str) -> DagsterCloudSandboxConnectionInfo:
        pass

    @abstractmethod
    def _remove_server_handle(self, server_handle: ServerHandle) -> None:
        """Shut down any resources associated with the given handle. Called both during updates
        to spin down the old server once a new server has been spun up, and during removal."""

    @abstractmethod
    def _cleanup_servers(self):
        """Remove all servers, across all locations."""

    @abstractmethod
    def get_step_handler(self, execution_config: Optional[Dict]) -> StepHandler:
        pass

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

    def update_grpc_metadata(
        self,
        desired_metadata: Dict[str, UserCodeLauncherEntry],
        upload_locations: Set[str],
    ):
        check.dict_param(
            desired_metadata,
            "desired_metadata",
            key_type=str,
            value_type=UserCodeLauncherEntry,
        )
        check.set_param(upload_locations, "upload_locations", str)

        with self._metadata_lock:
            # Change the inputs for the next reconciliation loop
            self._desired_entries = desired_metadata
            self._upload_locations = self._upload_locations.union(upload_locations)

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

        if self._reconcile_count == 1:
            with self._is_workspace_ready_lock:
                self._is_workspace_ready = True

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

    def _reconcile(self, desired_entries, upload_locations):
        diff = diff_serializable_namedtuple_map(desired_entries, self._actual_entries)

        to_update_keys = diff.to_add.union(diff.to_update)
        existing_server_handles: Dict[str, List[ServerHandle]] = {}
        for to_update_key in to_update_keys:
            try:

                code_deployment_metadata = desired_entries[to_update_key].code_deployment_metadata

                self._logger.info(
                    "Updating server for location {location_name}".format(
                        location_name=to_update_key
                    )
                )
                existing_server_handles[to_update_key] = self._get_server_handles_for_location(
                    to_update_key
                )

                self._check_for_image(code_deployment_metadata)

                if self.is_dev_sandbox():
                    if self._should_soft_reload(
                        location_name=to_update_key,
                        entry=desired_entries[to_update_key],
                    ):
                        new_updated_endpoint = self._soft_reload(to_update_key)
                    else:
                        new_updated_endpoint = self._create_dev_sandbox_endpoint(
                            to_update_key,
                            code_deployment_metadata,
                            authorized_key=self._authorized_key(),
                            proxy_info=desired_entries[to_update_key].sandbox_proxy_info,
                        )
                else:
                    new_updated_endpoint = self._create_new_server_endpoint(
                        to_update_key,
                        code_deployment_metadata,
                    )

                self._logger.info(
                    "Created a new server for location {location_name}".format(
                        location_name=to_update_key
                    )
                )
            except Exception:
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error(
                    "Error while updating server for {to_update_key}: {error_info}".format(
                        to_update_key=to_update_key,
                        error_info=error_info,
                    )
                )
                new_updated_endpoint: SerializableErrorInfo = error_info

            with self._grpc_endpoints_lock:
                self._grpc_endpoints[to_update_key] = new_updated_endpoint

            if to_update_key in upload_locations:
                upload_locations.remove(to_update_key)
                self._update_location_data(
                    to_update_key,
                    new_updated_endpoint,
                    desired_entries[to_update_key].code_deployment_metadata,
                )

        for to_update_key in to_update_keys:
            # If a hard reload, clean up the server handles
            # If a soft reload, don't because we changed them in place
            if not self._should_soft_reload(
                location_name=to_update_key,
                entry=desired_entries[to_update_key],
            ):
                server_handles = existing_server_handles.get(to_update_key, [])

                if server_handles:
                    self._logger.info(
                        "Removing {num_servers} existing servers for location {location_name}".format(
                            num_servers=len(server_handles), location_name=to_update_key
                        )
                    )

                for server_handle in server_handles:
                    try:
                        self._remove_server_handle(server_handle)
                    except Exception:
                        self._logger.error(
                            "Error while cleaning up after updating server for {to_update_key}: {error_info}".format(
                                to_update_key=to_update_key,
                                error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                            )
                        )

                if server_handles:
                    self._logger.info(
                        "Removed all previous servers for {location_name}".format(
                            location_name=to_update_key
                        )
                    )

            # Always update our actual entries
            self._actual_entries[to_update_key] = desired_entries[to_update_key]

        for to_remove_key in diff.to_remove:
            try:
                self._remove_server(to_remove_key)
            except Exception:
                self._logger.error(
                    "Error while removing server for {to_remove_key}: {error_info}".format(
                        to_remove_key=to_remove_key,
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

            self._update_location_data(
                location,
                endpoint,
                self._actual_entries[location].code_deployment_metadata,
            )

    def get_grpc_endpoint(
        self, repository_location_origin: RepositoryLocationOrigin
    ) -> GrpcServerEndpoint:
        with self._grpc_endpoints_lock:
            location_name = repository_location_origin.location_name
            endpoint = self._grpc_endpoints.get(location_name)

        if not endpoint:
            raise DagsterUserCodeUnreachableError(
                f"No server endpoint exists for location {location_name}"
            )

        if isinstance(endpoint, SerializableErrorInfo):
            # Consider raising the original exception here instead of a wrapped one
            raise DagsterUserCodeUnreachableError(
                f"Failure loading server endpoint for location {location_name}: {endpoint}"
            )

        return endpoint

    def get_grpc_endpoints(
        self,
    ) -> Dict[str, Union[GrpcServerEndpoint, SerializableErrorInfo]]:
        with self._grpc_endpoints_lock:
            return self._grpc_endpoints.copy()

    def _remove_server(self, location_name: str):
        self._logger.info(
            "Removing server for location {location_name}".format(location_name=location_name)
        )
        existing_server_handles = self._get_server_handles_for_location(location_name)
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

    def _should_soft_reload(self, location_name: str, entry: UserCodeLauncherEntry) -> bool:
        if not self.is_dev_sandbox():
            return False

        existing_entry = self._actual_entries.get(location_name)
        if not existing_entry:
            return False

        # A hard reload has been triggered
        if entry.update_timestamp > existing_entry.update_timestamp:
            return False

        if not existing_entry.sandbox_saved_timestamp:
            return True

        if entry.sandbox_saved_timestamp > existing_entry.sandbox_saved_timestamp:
            return True

        # Getting here shouldn't be possible
        return False

    def _soft_reload(self, to_update_key):
        existing_dev_sandbox_endpoint = self.get_grpc_endpoints().get(to_update_key)
        if existing_dev_sandbox_endpoint:
            server_id = existing_dev_sandbox_endpoint.server_id
            host = existing_dev_sandbox_endpoint.host
            port = existing_dev_sandbox_endpoint.port

            client = existing_dev_sandbox_endpoint.create_client()
            client.shutdown_server()

            start_time = time.time()
            last_error = f"Server {server_id} is still running."
            while True:
                try:
                    # Wait for the server ID to change; this means we've reloaded the gRPC server.
                    if server_id != sync_get_server_id(client):
                        break
                except Exception:
                    last_error = serializable_error_info_from_exc_info(sys.exc_info())
                if time.time() - start_time > 15:
                    raise Exception(
                        f"Timed out waiting for server {host}:{port} to reload. Most recent error: {str(last_error)}"
                    )
                time.sleep(1)
            new_updated_endpoint = GrpcServerEndpoint(
                server_id=sync_get_server_id(client),
                host=host,
                port=port,
                socket=None,
            )
            return new_updated_endpoint
