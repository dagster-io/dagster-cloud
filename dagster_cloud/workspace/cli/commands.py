import time

import click
import yaml
from dagster import check
from dagster.serdes.serdes import deserialize_json_to_dagster_namedtuple
from dagster.utils import DEFAULT_WORKSPACE_YAML_FILENAME
from dagster_cloud.api.client import create_cloud_dagit_client
from dagster_cloud.workspace.config_schema import process_workspace_config
from dagster_cloud.workspace.origin import CodeDeploymentMetadata

from ...cli.utils import add_options, create_cloud_dagit_client_options

_deployment_metadata_options = [
    click.option("--image", type=click.STRING, help="Docker image"),
    click.option(
        "--python-file",
        "-f",
        type=click.Path(exists=False),
        help="python file where repository lives",
    ),
    click.option(
        "--package-name",
        "-p",
        help="Python package where repositories live",
    ),
    click.option("--sha", type=click.STRING, help="Git sha"),
    click.option("--url", type=click.STRING, help="Git URL"),
]


ADD_LOCATION_MUTATION = """
mutation ($location: LocationSelector!) {
   addLocation(location: $location) {
      __typename
      ... on WorkspaceEntry {
        locationName
      }
      ... on PythonError {
        message
        stack
      }
   }
}
"""

REPOSITORY_LOCATIONS_QUERY = """
{
  workspaceOrError {
    __typename
    ... on Workspace {
      locationEntries {
        __typename
        name
        loadStatus
        locationOrLoadError {
          __typename
          ... on RepositoryLocation {
            name
          }
          ... on PythonError {
              message
              stack
          }
        }
      }
    }
    ... on PythonError {
      message
      stack
    }
  }
}
"""


@click.command(
    name="add-location",
    help="Add a new repository location to the workspace.",
)
@add_options(create_cloud_dagit_client_options)
@add_options(_deployment_metadata_options)
@click.argument("location", type=click.STRING)
def add_command(url, api_token, location, **kwargs):
    execute_add_command(create_cloud_dagit_client(url, api_token), location, kwargs)


def _get_location_input(location, kwargs):
    location_input = {"name": location}

    python_file = kwargs.get("python_file")
    package_name = kwargs.get("package_name")

    if (not python_file and not package_name) or (python_file and package_name):
        raise click.UsageError("Must specify exactly one of --python-file or --package-name.")

    if python_file:
        location_input["pythonFile"] = python_file
    if package_name:
        location_input["packageName"] = package_name
    if "image" in kwargs:
        location_input["image"] = kwargs["image"]

    return location_input


def execute_add_command(client, location, kwargs):
    variables = {"location": _get_location_input(location, kwargs)}

    click.echo(f"Adding location {location}...")

    add_res = client.execute(ADD_LOCATION_MUTATION, variable_values=variables)

    if add_res["data"]["addLocation"]["__typename"] == "WorkspaceEntry":
        click.echo(f"Added location {location}.")
    else:
        raise click.ClickException(f"Error adding location: {str(add_res)}")

    wait_for_load(client, [location])


def wait_for_load(client, locations):
    start_time = time.time()
    click.echo(f"Waiting for agent to sync changes to {','.join(locations)}...")
    while True:
        if time.time() - start_time > 180:
            raise click.ClickException("Timed out waiting for location data to update")

        repo_locations_res = client.execute(REPOSITORY_LOCATIONS_QUERY)

        if not "data" in repo_locations_res:
            raise click.ClickException(f"Error fetching locations: {str(repo_locations_res)}")
        locations_or_error = repo_locations_res["data"]["workspaceOrError"]
        if locations_or_error["__typename"] == "PythonError" or not "data":
            raise click.ClickException(f"Error fetching locations: {str(locations_or_error)}")

        nodes = repo_locations_res["data"]["workspaceOrError"]["locationEntries"]

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
                raise click.ClickException(error_string)
            else:
                click.echo(
                    f"Agent synced changes to {','.join(locations)}. Changes should now be visible in dagit."
                )
                break

        time.sleep(3)


UPDATE_LOCATION_MUTATION = """
mutation ($location: LocationSelector!) {
   updateLocation(location: $location) {
      __typename
      ... on WorkspaceEntry {
        locationName
      }
      ... on PythonError {
        message
        stack
      }
   }
}
"""


@click.command(
    name="update-location",
    help="Update the image for a repository location in the workspace.",
)
@add_options(create_cloud_dagit_client_options)
@add_options(_deployment_metadata_options)
@click.argument("location", type=click.STRING)
def update_command(url, api_token, location, **kwargs):
    execute_update_command(create_cloud_dagit_client(url, api_token), location, kwargs)


def execute_update_command(client, location, kwargs):
    click.echo(f"Updating location {location}...")

    variables = {"location": _get_location_input(location, kwargs)}

    update_res = client.execute(UPDATE_LOCATION_MUTATION, variable_values=variables)

    if update_res["data"]["updateLocation"]["__typename"] == "WorkspaceEntry":
        click.echo(f"Updated location {location}.")
    else:
        raise click.ClickException(f"Error adding location: {str(update_res)}")

    wait_for_load(client, [location])


DELETE_LOCATION_MUTATION = """
mutation ($locationName: String!) {
   deleteLocation(locationName: $locationName) {
      __typename
      ... on DeleteLocationSuccess {
        locationName
      }
      ... on PythonError {
        message
        stack
      }
   }
}
"""


@click.command(
    name="delete-location",
    help="Delete a repository location from the workspace.",
)
@add_options(create_cloud_dagit_client_options)
@click.argument("location", type=click.STRING)
def delete_command(url, api_token, location):
    execute_delete_command(create_cloud_dagit_client(url, api_token), location)


def execute_delete_command(client, location):
    click.echo(f"Deleting location {location}...")

    variables = {"locationName": location}

    delete_res = client.execute(DELETE_LOCATION_MUTATION, variable_values=variables)

    if delete_res["data"]["deleteLocation"]["__typename"] == "DeleteLocationSuccess":
        click.echo(f"Deleted location {location}")
    else:
        raise click.ClickException(f"Error deleting location: {str(delete_res)}")


LIST_LOCATIONS_QUERY = """
query WorkspaceEntries {
    workspace {
        workspaceEntries {
            locationName
            serializedDeploymentMetadata
        }
    }
}
"""


@click.command(
    name="list",
    help="List repository locations in the workspace.",
)
@add_options(create_cloud_dagit_client_options)
def list_command(url, api_token):
    execute_list_command(create_cloud_dagit_client(url, api_token))


def execute_list_command(client):
    click.echo("Listing locations...")

    list_res = client.execute(LIST_LOCATIONS_QUERY)

    for location in list_res["data"]["workspace"]["workspaceEntries"]:
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

        click.echo("\t".join(location_desc))


RECONCILE_LOCATIONS_MUTATION = """
mutation ($locations: [LocationSelector]!) {
    reconcileLocations(locations: $locations) {
        __typename
        ... on ReconcileLocationsSuccess {
            locations {
                locationName
            }
        }
        ... on PythonError {
            message
            stack
        }
    }
}
"""


@click.command(name="sync", help="Sync the workspace with the contents of a workspace.yaml file.")
@add_options(create_cloud_dagit_client_options)
@click.option(
    "--workspace",
    "-w",
    type=click.Path(exists=True),
    default=DEFAULT_WORKSPACE_YAML_FILENAME,
    help="Path to workspace file.",
)
def sync_command(url, api_token, workspace):
    execute_sync_command(create_cloud_dagit_client(url, api_token), workspace)


def execute_sync_command(client, workspace):
    with open(str(workspace), "r") as f:
        config = yaml.load(f.read(), Loader=yaml.SafeLoader)
        process_workspace_config(config)

    locations = {"locations": []}
    for name, metadata in config["locations"].items():
        locations["locations"].append(
            {
                "name": name,
                "image": metadata.get("image"),
                "packageName": metadata.get("package_name"),
                "pythonFile": metadata.get("python_file"),
            }
        )

    reconcile_res = client.execute(RECONCILE_LOCATIONS_MUTATION, variable_values=locations)

    if reconcile_res["data"]["reconcileLocations"]["__typename"] == "ReconcileLocationsSuccess":
        locations = sorted(
            [
                location["locationName"]
                for location in reconcile_res["data"]["reconcileLocations"]["locations"]
            ]
        )
        click.echo(f"Synced locations: {', '.join(locations)}")
    else:
        raise click.ClickException(f"Error syncing locations: {str(reconcile_res)}")

    wait_for_load(client, locations)
