from pathlib import Path
from typing import List

import yaml
from dagster.utils import merge_dicts
from typer import Argument, Typer

from .. import gql, ui
from ..config_utils import dagster_cloud_options

app = Typer(help="Customize your Dagster Cloud deployment.")

settings_app = Typer(help="Change your deployment settings.")
app.add_typer(settings_app, name="settings")


@settings_app.command(name="set-from-file")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def set_from_filecommand(
    api_token: str,
    url: str,
    file_path: Path = Argument(..., readable=True, metavar="SETTINGS_FILE_PATH"),
):
    """
    Set the Dagster Cloud deployment settings from a YAML file.
    """
    with open(file_path, "r") as f:
        settings = {"settings": yaml.safe_load(f) or {}}
    client = gql.graphql_client_from_url(url, api_token)
    gql.set_deployment_settings(client, settings)


@settings_app.command(name="get")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def get_command(
    api_token: str,
    url: str,
):
    """
    Get the Dagster Cloud deployment settings.
    """
    client = gql.graphql_client_from_url(url, api_token)
    settings = gql.get_deployment_settings(client)
    ui.print_yaml(settings)