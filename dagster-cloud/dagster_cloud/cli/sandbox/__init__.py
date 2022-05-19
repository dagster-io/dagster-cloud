# pylint: disable=unused-argument

from collections import Counter
import os
import subprocess
from datetime import timedelta
from typing import Dict, List, Optional
from dagster_cloud.cli.sandbox.utils import get_current_display_timestamp

import typer
import yaml
from dagster_cloud.api.dagster_cloud_api import DagsterCloudSandboxConnectionInfo
from dagster_cloud.cli.config_utils import dagster_cloud_options
from dagster_cloud.storage.client import GqlShimClient
from typer import Typer
import re
from .. import gql, ui
from .sync_method import MutagenSyncMethod, SyncMethod, SyncState, SyncedDirectory

CONNECTION_INFO_GIVE_UP_TIME = timedelta(seconds=120)

app = Typer(help="Interface with your dev sandbox.")


def get_all_connection_info(client: GqlShimClient) -> Dict[str, DagsterCloudSandboxConnectionInfo]:
    """
    Returns a mapping of location name to sandbox connection info for all locations.
    """
    workspace_entries = gql.fetch_workspace_entries(client)
    return {
        workspace_entry["locationName"]: DagsterCloudSandboxConnectionInfo(
            username=workspace_entry["connectionInfo"]["username"],
            hostname=workspace_entry["connectionInfo"]["hostname"],
            port=workspace_entry["connectionInfo"]["port"],
        )
        for workspace_entry in workspace_entries
        if workspace_entry.get("connectionInfo")
    }


def launch_ssh(arguments: List[str]) -> None:
    os.execvp("ssh", ["ssh", *arguments])


def get_default_sync_method() -> SyncMethod:
    return MutagenSyncMethod()


def dev_deployment_client_or_error(
    base_client_url: str, organization: str, api_token: str
) -> GqlShimClient:
    default_client = gql.graphql_client_from_url(base_client_url, api_token)
    try:
        dev_depl = gql.get_dev_deployment_name(default_client)
    except Exception:
        raise ui.error(
            "No dev sandbox found for your user - you need to set one up before you can use this command."
        )
    # Current deployment is dev depl
    if not dev_depl:
        return default_client

    # Else, construct new gql client
    return gql.graphql_client_from_url(gql.url_from_config(organization, dev_depl), api_token)


@app.command(name="ssh")
@dagster_cloud_options(requires_url=True)
def ssh(
    organization: str,
    deployment: str,
    api_token: str,
    url: str,
    location_name=typer.Argument(
        ..., help="The name of the code location whose user code server to connect to."
    ),
) -> None:
    """
    Connects to the user code server hosting a given code location.
    """
    client = dev_deployment_client_or_error(url, organization, api_token)

    all_connection_info = get_all_connection_info(client)

    if not location_name in all_connection_info:
        raise ui.error(f"Cannot find sandbox user code server for location {location_name}.")

    connection_info = all_connection_info[location_name]

    # TODO forward all other arguments to SSH
    launch_ssh(
        ["-p", str(connection_info.port), f"{connection_info.username}@{connection_info.hostname}"]
    )


def find_locations_yaml(search_directory: str) -> Optional[str]:
    """
    Given a search directory, finds a locations.yaml file located
    in the search directory or a parent. Returns None if none is found.
    """
    curr_dir = search_directory

    while not os.path.exists(os.path.join(curr_dir, "locations.yaml")):
        parent_dir = os.path.dirname(curr_dir)
        if parent_dir == curr_dir:
            return None
        curr_dir = parent_dir
    return curr_dir


def get_directory_identifier(location_name: str, index: int) -> str:
    return f"{re.sub('[^0-9a-zA-Z-]+', '-', location_name)}-{index}"


def get_directories_to_sync(
    directory: str, all_connection_info: Dict[str, DagsterCloudSandboxConnectionInfo]
) -> List[SyncedDirectory]:
    """
    Finds a set of code sync mappings defined in a locations.yaml file,
    combines with the sandbox connection info to create a list of directories to sync.
    """
    locations_yaml_dir = find_locations_yaml(directory)
    if not locations_yaml_dir:
        raise ValueError("No locations.yaml file found")

    directories_to_sync: List[SyncedDirectory] = []
    with open(os.path.join(locations_yaml_dir, "locations.yaml"), encoding="utf8") as f:
        locations = yaml.safe_load(f.read())["locations"]

        defined_locations = set(locations.keys())
        available_locations = set(all_connection_info.keys())

        if len(available_locations.intersection(defined_locations)) == 0:
            defined_locations_display = ", ".join(locations.keys()) or "(None)"
            available_locations_display = ", ".join(all_connection_info.keys()) or "(None)"
            raise ui.error(
                "No accessable sync locations found\n\n"
                f"Defined in locations.yaml: {defined_locations_display}\nAvailable code locations: {available_locations_display}"
            )

        if len(defined_locations - available_locations) > 0:
            not_synced = ", ".join(defined_locations - available_locations)
            ui.warn(
                f"Some locations defined in locations.yaml are not accessible to sync: {not_synced}\n"
            )

        if len(available_locations - defined_locations) > 0:
            not_synced = ", ".join(available_locations - defined_locations)
            ui.warn(
                f"Some available locations are not defined in locations.yaml, and will not be synced: {not_synced}\n"
            )

        for location_name, location in locations.items():
            if location_name in all_connection_info:
                connection_info = all_connection_info[location_name]
                for i, mapping in enumerate(location.get("code_sync", [])):
                    directories_to_sync.append(
                        SyncedDirectory(
                            identifier=get_directory_identifier(location_name, i),
                            location_name=location_name,
                            from_directory=str(
                                os.path.join(locations_yaml_dir, mapping.get("from"))
                            ),
                            to_directory=mapping.get("to"),
                            connection_info=connection_info,
                        )
                    )
    return directories_to_sync


def validate_host_connection(connection_info: DagsterCloudSandboxConnectionInfo):
    """
    Validates that we can SSH into the given host, and prompts the user to add to
    known_hosts if need be.
    """
    subprocess.check_output(
        [
            "ssh",
            "-p",
            f"{connection_info.port}",
            "-o",
            "ConnectTimeout=10",
            f"{connection_info.username}@{connection_info.hostname}",
            "exit",
        ]
    )


def print_sync_state(state_map: Dict[str, SyncState], remove_previous: bool = True):
    """
    Given a mapping from directory identifiers to sync states, prints a summary of
    all sync states.
    """
    if remove_previous:
        ui.erase_previous_line(2)

    c = Counter(state_map.values())
    n_unavailable = c.get(SyncState.UNAVAILABLE, 0)
    n_syncing = c.get(SyncState.SYNCING, 0)
    n_synced = c.get(SyncState.SYNCED, 0)
    total = len(state_map.keys()) - n_unavailable

    if n_syncing > 0:
        ui.print(
            ui.yellow(
                f"[{get_current_display_timestamp()}] ({n_synced}/{total}) Syncing directories..."
            )
        )
    else:
        ui.print(ui.blue(f"[{get_current_display_timestamp()}] ({n_synced}/{total}) Sync complete"))

    if n_unavailable > 0:
        ui.print(ui.red(f"           {n_unavailable} directories unable to sync"))
    elif remove_previous:
        ui.print()


@app.command(name="sync")
@dagster_cloud_options(requires_url=True)
def code_sync(
    organization: str,
    deployment: str,
    api_token: str,
    url: str,
    verbose: bool = typer.Option(False, "--verbose", "-v", is_flag=True),
    very_verbose: bool = typer.Option(False, "--very-verbose", "-vv", is_flag=True),
):
    """
    Starts to synchronize code in the local environment with code in your cloud sandbox.
    """
    if verbose and very_verbose:
        raise ui.error(
            f"Cannot specify both {ui.as_code('--verbose')} and {ui.as_code('--very-verbose')} options"
        )
    verbosity_level = 2 if very_verbose else 1 if verbose else 0

    client = dev_deployment_client_or_error(url, organization, api_token)

    sync_method = get_default_sync_method()
    sync_method.preflight(verbosity_level)

    # Populate list of locations to sync
    all_connection_info = get_all_connection_info(client)
    for connection_info in all_connection_info.values():
        try:
            validate_host_connection(connection_info)
        except Exception:
            raise ui.error("Unable to establish connection")

    directories_to_sync = get_directories_to_sync(os.path.abspath(os.getcwd()), all_connection_info)

    ui.print("Enabling code sync...")

    for directory in directories_to_sync:
        sync_method.create_directory_sync(directory)

    identifier_location_name_map = {
        directory.identifier: directory.location_name for directory in directories_to_sync
    }
    try:
        states_map = {dir.identifier: SyncState.SYNCING for dir in directories_to_sync}

        print()
        print_sync_state(states_map, remove_previous=True)

        for identifier, new_state in sync_method.sync_loop():
            last_state = states_map[identifier]
            states_map[identifier] = new_state

            if new_state == SyncState.SYNCED and last_state != new_state:
                gql.reload_repo_location(client, identifier_location_name_map[identifier])

            if (
                new_state == SyncState.SYNCING
                or new_state == SyncState.UNAVAILABLE
                or last_state != new_state
            ):
                print_sync_state(states_map, remove_previous=verbosity_level == 0)

    except KeyboardInterrupt:
        pass
    finally:
        ui.print("Terminating sync...")
        for directory in directories_to_sync:
            sync_method.cleanup_directory_sync(directory.identifier)
