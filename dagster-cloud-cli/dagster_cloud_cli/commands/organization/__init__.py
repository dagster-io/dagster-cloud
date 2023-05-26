from pathlib import Path

import yaml
from typer import Argument, Typer

from ... import gql, ui
from ...config_utils import dagster_cloud_options
from .saml import commands as saml_cli

app = Typer(help="Customize your Dagster Cloud organization.")
settings_app = Typer(help="Customize your organization settings.")

app.add_typer(settings_app, name="settings", no_args_is_help=True)
settings_app.add_typer(saml_cli.app, name="saml", no_args_is_help=True)

# Legacy, to support the old `dagster-cloud settings` path
# New command is `dagster-cloud organization
legacy_settings_app = Typer(
    help=(
        "[Deprecated, in favor of dagster-cloud organization] Customize your dagster-cloud"
        " settings."
    ),
    hidden=True,
)
legacy_settings_app.add_typer(saml_cli.app, name="saml")


@settings_app.command(name="set-from-file")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def set_from_filecommand(
    organization: str,
    api_token: str,
    url: str,
    file_path: Path = Argument(..., readable=True, metavar="SETTINGS_FILE_PATH"),
):
    """Set the Dagster Cloud organization settings from a YAML file."""
    if not url and not organization:
        raise ui.error("Must provide either organization name or URL.")
    if not url:
        url = gql.url_from_config(organization=organization)

    with open(file_path, "r", encoding="utf8") as f:
        settings = {"settings": yaml.safe_load(f) or {}}
    with gql.graphql_client_from_url(url, api_token) as client:
        gql.set_organization_settings(client, settings)


@settings_app.command(name="get")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def get_command(
    organization: str,
    api_token: str,
    url: str,
):
    """Get the Dagster Cloud organization settings."""
    if not url and not organization:
        raise ui.error("Must provide either organization name or URL.")
    if not url:
        url = gql.url_from_config(organization=organization)

    with gql.graphql_client_from_url(url, api_token) as client:
        settings = gql.get_organization_settings(client)
        ui.print_yaml(settings)
