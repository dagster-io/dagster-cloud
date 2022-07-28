import os
import sys
import tempfile
import zlib
from contextlib import contextmanager
from typing import Any, Dict

import requests
from dagster._core.host_representation import InProcessRepositoryLocation
from dagster._core.host_representation.origin import InProcessRepositoryLocationOrigin
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._serdes import serialize_dagster_namedtuple
from dagster._utils.error import serializable_error_info_from_exc_info
from dagster_cloud.api.dagster_cloud_api import (
    DagsterCloudUploadLocationData,
    DagsterCloudUploadRepositoryData,
    DagsterCloudUploadWorkspaceEntry,
)
from dagster_cloud_cli import gql, ui
from dagster_cloud_cli.config_utils import DEPLOYMENT_METADATA_OPTIONS, dagster_cloud_options
from dagster_cloud_cli.core.headers.auth import DagsterCloudInstanceScope
from dagster_cloud_cli.core.headers.impl import get_dagster_cloud_api_headers
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata
from dagster_cloud_cli.utils import add_options
from typer import Argument


def _get_location_input(location: str, kwargs: Dict[str, Any]) -> gql.CliInputCodeLocation:
    python_file = kwargs.get("python_file")

    return gql.CliInputCodeLocation(
        name=location,
        python_file=str(python_file) if python_file else None,
        package_name=kwargs.get("package_name"),
        image=kwargs.get("image"),
        module_name=kwargs.get("module_name"),
        working_directory=kwargs.get("working_directory"),
        executable_path=kwargs.get("executable_path"),
        attribute=kwargs.get("attribute"),
        commit_hash=kwargs["git"].get("commit_hash")
        if "git" in kwargs
        else kwargs.get("commit_hash"),
        url=kwargs["git"].get("url") if "git" in kwargs else kwargs.get("git_url"),
    )


@dagster_cloud_options(allow_empty=True, requires_url=True)
@add_options(DEPLOYMENT_METADATA_OPTIONS)
def snapshot_command(
    api_token: str,
    url: str,
    location: str = Argument(..., help="Code location name."),
    code_preview_uuid: str = Argument(..., help="The code preview that will own this snapshot."),
    **kwargs,
):
    """Generate a snapshot for a code preview."""
    location_data = _get_location_input(location, kwargs)

    # 0. Load location
    with get_location_or_load_error(location_data) as (repository_location, error_info):
        deployment_metadata = CodeDeploymentMetadata(
            image=location_data.image,
            python_file=location_data.python_file,
            package_name=location_data.package_name,
            module_name=location_data.module_name,
            working_directory=location_data.working_directory,
            executable_path=location_data.executable_path,
            attribute=location_data.attribute,
        )

        # 1. Record any serialization errors, if any
        if error_info:
            upload_workspace_entry = DagsterCloudUploadWorkspaceEntry(
                location_name=location_data.name,
                deployment_metadata=deployment_metadata,
                upload_location_data=None,
                serialized_error_info=error_info,
            )
        else:
            # 2. Get location, get repository datas from each repository.
            upload_repository_datas = [
                DagsterCloudUploadRepositoryData(
                    repository_name=repository_name,
                    code_pointer=code_pointer,
                    serialized_repository_data=serialize_dagster_namedtuple(
                        repository_location.get_repository(repository_name).external_repository_data
                    ),
                )
                for repository_name, code_pointer in repository_location.repository_code_pointer_dict.items()
            ]

            # 3. Construct DagsterCloudUploadLocationData
            upload_location_data = DagsterCloudUploadLocationData(
                upload_repository_datas=upload_repository_datas,
                container_image=location_data.image,
                executable_path=repository_location.executable_path,
            )

            # 4. Construct DagsterCloudUploadWorkspaceEntry, upload via file upload endpoint
            upload_workspace_entry = DagsterCloudUploadWorkspaceEntry(
                location_name=location_data.name,
                deployment_metadata=deployment_metadata,
                upload_location_data=upload_location_data,
                serialized_error_info=None,
            )

        # 5. Upload DagsterCloudUploadWorkspaceEntry via file upload endpoint
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                dst = os.path.join(temp_dir, "workspace_entry.tmp")
                with open(dst, "wb") as f:
                    serialized_workspace_entry = serialize_dagster_namedtuple(
                        upload_workspace_entry
                    )
                    f.write(zlib.compress(serialized_workspace_entry.encode("utf-8")))

                with open(dst, "rb") as f:
                    response = requests.post(
                        url=f"{url}/upload_code_preview_workspace_entry/{code_preview_uuid}",
                        headers=get_dagster_cloud_api_headers(
                            api_token, DagsterCloudInstanceScope.DEPLOYMENT
                        ),
                        files={"workspace_entry.tmp": f},
                    )
                    response.raise_for_status()

            ui.print(
                (
                    f"Uploaded snapshot of location `{location_data.name}` "
                    f"in image `{location_data.image}` for code preview `{code_preview_uuid}`."
                )
            )
        except Exception as e:
            raise ui.error(
                f"Failed to upload snapshot of location `{location_data.name}` "
                f"in image `{location_data.image}` for code preview `{code_preview_uuid}`: "
                f"{str(e)}"
            )


@contextmanager
def get_location_or_load_error(location_data: gql.CliInputCodeLocation):
    loadable_target_origin = LoadableTargetOrigin(
        executable_path=location_data.executable_path
        if location_data.executable_path
        else sys.executable,
        python_file=location_data.python_file,
        package_name=location_data.package_name,
        module_name=location_data.module_name,
        working_directory=location_data.working_directory or os.getcwd(),
        attribute=location_data.attribute,
    )
    origin = InProcessRepositoryLocationOrigin(loadable_target_origin)
    try:
        with InProcessRepositoryLocation(origin) as location:
            yield (location, None)
    except Exception:
        load_error = serializable_error_info_from_exc_info(sys.exc_info())
        yield (None, load_error)
