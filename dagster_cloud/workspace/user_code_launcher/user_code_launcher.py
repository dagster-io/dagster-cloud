import os
import sys
import tempfile
import threading
import time
import zlib
from abc import abstractmethod
from typing import Dict, Optional, Set

from dagster import check
from dagster.api.get_server_id import sync_get_server_id
from dagster.api.list_repositories import sync_list_repositories_grpc
from dagster.core.executor.step_delegating import StepHandler
from dagster.core.host_representation import ExternalRepositoryOrigin
from dagster.core.host_representation.grpc_server_registry import GrpcServerRegistry
from dagster.core.host_representation.origin import RegisteredRepositoryLocationOrigin
from dagster.core.instance import MayHaveInstanceWeakref
from dagster.daemon.daemon import get_default_daemon_logger
from dagster.grpc.client import DagsterGrpcClient
from dagster.grpc.types import GetCurrentImageResult
from dagster.serdes import deserialize_json_to_dagster_namedtuple, serialize_dagster_namedtuple
from dagster.utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster_cloud.api.dagster_cloud_api import (
    DagsterCloudUploadLocationData,
    DagsterCloudUploadRepositoryData,
    DagsterCloudUploadWorkspaceEntry,
)
from dagster_cloud.errors import raise_http_error
from dagster_cloud.execution.watchful_run_launcher.base import WatchfulRunLauncher
from dagster_cloud.storage.client import create_agent_requests_session
from dagster_cloud.util import diff_serializable_namedtuple_map
from dagster_cloud.workspace.origin import CodeDeploymentMetadata

USER_CODE_LAUNCHER_RECONCILE_INTERVAL = 1


class DagsterCloudUserCodeLauncher(GrpcServerRegistry, MayHaveInstanceWeakref):
    def __init__(self):
        self._logger = get_default_daemon_logger("DagsterUserCodeLauncher")
        self._started = False

    def start(self):
        check.invariant(
            not self._started,
            "Called start() on a DagsterCloudUserCodeLauncher that was already started",
        )
        # Begin spinning user code up and down
        self._started = True

    @abstractmethod
    def update_grpc_metadata(
        self, desired_metadata: Dict[str, CodeDeploymentMetadata], force_update_locations: Set[str]
    ):
        pass

    def supports_origin(self, repository_location_origin):
        return isinstance(repository_location_origin, RegisteredRepositoryLocationOrigin)

    @property
    def supports_reload(self):
        return False

    def reload_grpc_endpoint(self, repository_location_origin):
        raise NotImplementedError("Call update_grpc_metadata to update gRPC endpoints")

    @abstractmethod
    def _get_repository_location_origin(self, location_name):
        pass

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
                with create_agent_requests_session() as session:
                    resp = session.post(
                        self._instance.dagster_cloud_upload_workspace_entry_url,
                        headers=self._instance.dagster_cloud_api_headers,
                        files={"workspace_entry.tmp": f},
                    )
                    raise_http_error(resp)
                    self._logger.info(
                        "Successfully uploaded workspace entry for {location_name}".format(
                            location_name=workspace_entry.location_name
                        )
                    )

    def _get_upload_location_data(self, location_name):
        location_origin = self._get_repository_location_origin(location_name)
        client = self.get_grpc_endpoint(location_origin).create_client()

        list_repositories_response = sync_list_repositories_grpc(client)
        repository_code_pointer_dict = list_repositories_response.repository_code_pointer_dict
        executable_path = list_repositories_response.executable_path
        container_image = check.inst(
            deserialize_json_to_dagster_namedtuple(client.get_current_image()),
            GetCurrentImageResult,
        ).current_image

        upload_repo_datas = []

        for repository_name, code_pointer in repository_code_pointer_dict.items():
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
            container_image=container_image,
            executable_path=executable_path,
        )

    def _update_location_error(self, location_name, error_info, metadata):
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

    def _update_location_data(self, location_name, endpoint, metadata):
        self._logger.info(
            "Updating data for location {location_name}".format(location_name=location_name)
        )

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
        except Exception:  # pylint: disable=broad-except
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


class ReconcileUserCodeLauncher(DagsterCloudUserCodeLauncher):
    def __init__(self):
        self._grpc_endpoints = {}
        self._grpc_endpoints_lock = threading.Lock()
        self._logger = get_default_daemon_logger("ReconcileUserCodeLauncher")

        # periodically reconciles to make desired = actual
        self._desired_metadata = {}
        self._actual_metadata = {}
        self._force_update_keys = set()
        self._metadata_lock = threading.Lock()

        super().__init__()

        self._reconcile_count = 0
        self._reconcile_grpc_metadata_shutdown_event = threading.Event()
        self._reconcile_grpc_metadata_thread = None

    def start(self):
        super().start()
        self._reconcile_grpc_metadata_thread = threading.Thread(
            target=self._reconcile_thread,
            args=(self._reconcile_grpc_metadata_shutdown_event,),
            name="grpc-reconcile-watch",
        )
        self._reconcile_grpc_metadata_thread.daemon = True
        self._reconcile_grpc_metadata_thread.start()

    def __exit__(self, exception_type, exception_value, traceback):
        if self._reconcile_grpc_metadata_thread:
            self._reconcile_grpc_metadata_shutdown_event.set()
            self._reconcile_grpc_metadata_thread.join()

        super().__exit__(exception_value, exception_value, traceback)

    def update_grpc_metadata(self, desired_metadata, force_update_locations):
        check.dict_param(
            desired_metadata, "desired_metadata", key_type=str, value_type=CodeDeploymentMetadata
        )
        check.set_param(force_update_locations, "force_update_locations", str)
        with self._metadata_lock:
            self._desired_metadata = desired_metadata
            self._force_update_keys = force_update_locations

    def _get_repository_location_origin(self, location_name):
        return RegisteredRepositoryLocationOrigin(location_name)

    def _reconcile_thread(self, shutdown_event):
        while True:
            shutdown_event.wait(USER_CODE_LAUNCHER_RECONCILE_INTERVAL)
            if shutdown_event.is_set():
                break

            try:
                self.reconcile()
            except Exception:  # pylint: disable=broad-except
                self._logger.error(
                    "Failure updating user code servers: {exc_info}".format(
                        exc_info=sys.exc_info(),
                    )
                )

    def reconcile(self):
        with self._metadata_lock:
            self._reconcile()
            self._reconcile_count += 1

    def _reconcile(self):
        diff = diff_serializable_namedtuple_map(
            self._desired_metadata, self._actual_metadata, self._force_update_keys
        )

        for to_add_key in diff.to_add:
            try:
                new_endpoint = self._add_server(to_add_key, self._desired_metadata[to_add_key])
            except Exception:  # pylint: disable=broad-except
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error(
                    "Error while adding server for {to_add_key}: {error_info}".format(
                        to_add_key=to_add_key,
                        error_info=error_info,
                    )
                )
                new_endpoint = error_info

            with self._grpc_endpoints_lock:
                self._grpc_endpoints[to_add_key] = new_endpoint

            try:
                self._update_location_data(
                    to_add_key, new_endpoint, self._desired_metadata[to_add_key]
                )
            except Exception:  # pylint: disable=broad-except
                # Don't let a failure uploading the serialized data keep other locations
                # from being updated or continue reconciling in a loop
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error(
                    "Error while writing location data for added server {to_add_key}: {error_info}".format(
                        to_add_key=to_add_key,
                        error_info=error_info,
                    )
                )

            self._actual_metadata[to_add_key] = self._desired_metadata[to_add_key]

        for to_remove_key in diff.to_remove:
            try:
                self._remove_server(to_remove_key, self._actual_metadata[to_remove_key])
            except Exception:  # pylint: disable=broad-except
                self._logger.error(
                    "Error while removing server for {to_remove_key}: {error_info}".format(
                        to_remove_key=to_remove_key,
                        error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                    )
                )

            with self._grpc_endpoints_lock:
                del self._grpc_endpoints[to_remove_key]
            del self._actual_metadata[to_remove_key]

        update_gens = {}
        for to_update_key in diff.to_update:
            update_gens[to_update_key] = self._gen_update_server(
                to_update_key,
                self._actual_metadata[to_update_key],
                self._desired_metadata[to_update_key],
            )
            try:
                new_updated_endpoint = next(update_gens[to_update_key])
            except Exception:  # pylint: disable=broad-except
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error(
                    "Error while updating server for {to_update_key}: {error_info}".format(
                        to_update_key=to_update_key,
                        error_info=error_info,
                    )
                )
                new_updated_endpoint = error_info

            with self._grpc_endpoints_lock:
                self._grpc_endpoints[to_update_key] = new_updated_endpoint

            try:
                self._update_location_data(
                    to_update_key, new_updated_endpoint, self._desired_metadata[to_update_key]
                )
            except Exception:  # pylint: disable=broad-except
                # Don't let a failure uploading the serialized data keep other locations
                # from being updated or continue reconciling in a loop
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                self._logger.error(
                    "Error while writing location data for updated server {to_update_key}: {error_info}".format(
                        to_update_key=to_update_key,
                        error_info=error_info,
                    )
                )

        for to_update_key, update_gen in update_gens.items():
            # Finish any remaining cleanup
            try:
                list(update_gen)
            except Exception:  # pylint: disable=broad-except
                self._logger.error(
                    "Error while cleaning up after updating server for {to_update_key}: {error_info}".format(
                        to_update_key=to_update_key,
                        error_info=serializable_error_info_from_exc_info(sys.exc_info()),
                    )
                )

            self._actual_metadata[to_update_key] = self._desired_metadata[to_update_key]
            if to_update_key in self._force_update_keys:
                self._force_update_keys.remove(to_update_key)

    def get_grpc_endpoint(self, repository_location_origin):
        with self._grpc_endpoints_lock:
            location_name = repository_location_origin.location_name
            endpoint = self._grpc_endpoints.get(location_name)

        if not endpoint:
            raise Exception(f"No server endpoint exists for location {location_name}")

        if isinstance(endpoint, SerializableErrorInfo):
            # Consider raising the original exception here instead of a wrapped one
            raise Exception(
                f"Failure loading server endpoint for location {location_name}: {endpoint}"
            )

        return endpoint

    def get_grpc_endpoints(self):
        with self._grpc_endpoints_lock:
            return self._grpc_endpoints.copy()

    @abstractmethod
    def _add_server(self, location_name, metadata):
        # Add a server for this location / metadata combination. This method should be idempotent
        # it's possible if will be called and the server already exists if an update failed
        # part-way through.
        pass

    @abstractmethod
    def _gen_update_server(self, location_name, old_metadata, new_metadata):
        # Update the server for the given location. Is a generator - should yield the new
        # GrpcServerEndpoint, then clean up any no longer needed resources
        pass

    @abstractmethod
    def _remove_server(self, location_name, metadata):
        pass

    def _wait_for_server(self, host, port, timeout=15, socket=None):
        # Wait for the server to be ready (while also loading the server ID)
        server_id = None
        start_time = time.time()
        while True:
            client = DagsterGrpcClient(port=port, host=host, socket=socket)
            try:
                server_id = sync_get_server_id(client)
                break
            except Exception:  # pylint: disable=broad-except
                pass

            if time.time() - start_time > timeout:
                raise Exception(f"Timed out waiting for server {host}:{port}")

            time.sleep(1)
        return server_id

    @abstractmethod
    def get_step_handler(self, execution_config: Optional[Dict]) -> StepHandler:
        pass

    @abstractmethod
    def run_launcher(self) -> WatchfulRunLauncher:
        pass
