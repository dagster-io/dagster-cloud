from pathlib import Path

import yaml
from typer import Argument, Typer

from ... import gql, ui
from ...config_utils import dagster_cloud_options
from ...utils import create_stub_app

try:
    from .alert_policies.commands import app as alert_policies_app
except ImportError:
    alert_policies_app = create_stub_app("dagster-cloud")

app = Typer(help="Customize your Dagster Cloud deployment.")

settings_app = Typer(help="Customize your deployment settings.")
app.add_typer(settings_app, name="settings", no_args_is_help=True)
app.add_typer(alert_policies_app, name="alert-policies", no_args_is_help=True)


@settings_app.command(name="set-from-file")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def set_from_filecommand(
    api_token: str,
    url: str,
    file_path: Path = Argument(..., readable=True, metavar="SETTINGS_FILE_PATH"),
):
    """Set the Dagster Cloud deployment settings from a YAML file."""
    with open(file_path, "r", encoding="utf8") as f:
        settings = {"settings": yaml.safe_load(f) or {}}
    with gql.graphql_client_from_url(url, api_token) as client:
        gql.set_deployment_settings(client, settings)


@settings_app.command(name="get")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def get_command(
    api_token: str,
    url: str,
):
    """Get the Dagster Cloud deployment settings."""
    with gql.graphql_client_from_url(url, api_token) as client:
        settings = gql.get_deployment_settings(client)
    ui.print_yaml(settings)
