import time
from pathlib import Path
from typing import Any, Dict
from dagster_cloud.storage.client import GqlShimClient

import yaml
from dagster import check
from dagster.serdes.serdes import deserialize_json_to_dagster_namedtuple
from dagster.utils import DEFAULT_WORKSPACE_YAML_FILENAME
from dagster_cloud.workspace.config_schema import process_workspace_config
from dagster_cloud.workspace.origin import CodeDeploymentMetadata
from typer import Argument, Option, Typer

from ...cli import gql, ui
from ...cli.config_utils import dagster_cloud_options
from ...cli.utils import add_options

app = Typer(help="Commands for managing a dagster cloud workspace.")

_DEPLOYMENT_METADATA_OPTIONS = {
    "image": (str, Option(None, "--image", help="Docker image.")),
    "python_file": (
        Path,
        Option(
            None, "--python-file", "-f", exists=False, help="Python file where repository lives."
        ),
    ),
    "package_name": (
        str,
        Option(None, "--package-name", "-p", help="Python package where repositories live"),
    ),
}


def _get_location_input(location: str, kwargs: Dict[str, Any]) -> gql.CliInputCodeLocation:
    python_file = kwargs.get("python_file")
    return gql.CliInputCodeLocation(
        name=location,
        python_file=str(python_file) if python_file else None,
        package_name=kwargs.get("package_name"),
        image=kwargs.get("image"),
    )


def _add_or_update_location(client: GqlShimClient, location_data: gql.CliInputCodeLocation) -> None:
    try:
        gql.add_or_update_code_location(client, location_data)
        ui.print(f"Added or updated location {location_data.name}.")
        wait_for_load(client, [location_data.name])
    except Exception as e:
        raise ui.error(str(e))


@app.command(name="add-location")
@dagster_cloud_options(allow_empty=True, requires_url=True)
@add_options(_DEPLOYMENT_METADATA_OPTIONS)
def add_command(
    api_token: str,
    url: str,
    location: str = Argument(..., help="Code location name."),
    **kwargs,
):
    """Add or update the image for a repository location in the workspace."""
    client = gql.graphql_client_from_url(url, api_token)
    location_data = _get_location_input(location, kwargs)
    _add_or_update_location(client, location_data)


@app.command(
    name="update-location",
)
@dagster_cloud_options(allow_empty=True, requires_url=True)
@add_options(_DEPLOYMENT_METADATA_OPTIONS)
def update_command(
    api_token: str,
    url: str,
    location: str = Argument(..., help="Code location name."),
    **kwargs,
):
    """Update the image for a repository location in the workspace."""
    client = gql.graphql_client_from_url(url, api_token)
    location_data = _get_location_input(location, kwargs)
    _add_or_update_location(client, location_data)


def wait_for_load(client, locations):
    start_time = time.time()
    ui.print(f"Waiting for agent to sync changes to {','.join(locations)}...")
    while True:
        if time.time() - start_time > 300:
            raise ui.error("Timed out waiting for location data to update")

        try:
            nodes = gql.fetch_code_locations(client)
        except Exception as e:
            raise ui.error(str(e))

        nodes_by_location = {node["name"]: node for node in nodes}

        if all(
            location in nodes_by_location
            and nodes_by_location[location].get("loadStatus") == "LOADED"
            for location in locations
        ):

            error_locations = [
                location
                for location in locations
                if "locationOrLoadError" in nodes_by_location[location]
                and nodes_by_location[location]["locationOrLoadError"]["__typename"]
                == "PythonError"
            ]

            if error_locations:
                error_string = "Some locations failed to load after being synced by the agent:\n" + "\n".join(
                    [
                        f"Error loading {error_location}: {str(nodes_by_location[error_location]['locationOrLoadError'])}"
                        for error_location in error_locations
                    ]
                )
                raise ui.error(error_string)
            else:
                ui.print(
                    f"Agent synced changes to {','.join(locations)}. Changes should now be visible in dagit."
                )
                break

        time.sleep(3)


@app.command(
    name="delete-location",
)
@dagster_cloud_options(allow_empty=True, requires_url=True)
def delete_command(
    api_token: str,
    url: str,
    location: str = Argument(..., help="Code location name."),
):
    """Delete a repository location from the workspace."""
    client = gql.graphql_client_from_url(url, api_token)
    try:
        gql.delete_code_location(client, location)
        ui.print(f"Deleted location {location}.")
    except Exception as e:
        raise ui.error(str(e))


@app.command(
    name="list",
)
@dagster_cloud_options(allow_empty=True, requires_url=True)
def list_command(
    url: str,
    api_token: str,
):
    """List repository locations in the workspace."""
    client = gql.graphql_client_from_url(url, api_token)
    execute_list_command(client)


def execute_list_command(client):
    list_res = gql.fetch_workspace_entries(client)

    ui.print("Listing locations...")

    for location in list_res:
        metadata = check.inst(
            deserialize_json_to_dagster_namedtuple(location["serializedDeploymentMetadata"]),
            CodeDeploymentMetadata,
        )

        location_desc = [location["locationName"]]
        if metadata.python_file:
            location_desc.append(f"File: {metadata.python_file}")
        if metadata.package_name:
            location_desc.append(f"Package: {metadata.package_name}")
        if metadata.image:
            location_desc.append(f"Image: {metadata.image}")

        ui.print("\t".join(location_desc))


@app.command(name="sync")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def sync_command(
    url: str,
    api_token: str,
    workspace: Path = Option(
        DEFAULT_WORKSPACE_YAML_FILENAME,
        "--workspace",
        "-w",
        exists=True,
        help="Path to workspace file.",
    ),
):
    """"Sync the workspace with the contents of a workspace.yaml file."""
    client = gql.graphql_client_from_url(url, api_token)
    execute_sync_command(client, workspace)


def execute_sync_command(client, workspace):
    with open(str(workspace), "r") as f:
        config = yaml.load(f.read(), Loader=yaml.SafeLoader)
        process_workspace_config(config)

    try:
        locations = gql.reconcile_code_locations(
            client,
            [
                gql.CliInputCodeLocation(
                    name=name,
                    image=m.get("image"),
                    package_name=m.get("package_name"),
                    python_file=m.get("python_file"),
                )
                for name, m in config["locations"].items()
            ],
        )
        ui.print(f"Synced locations: {', '.join(locations)}")
        wait_for_load(client, locations)
    except Exception as e:
        raise ui.error(str(e))
