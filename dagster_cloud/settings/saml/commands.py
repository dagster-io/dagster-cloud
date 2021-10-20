import click
import requests

from ...cli.utils import add_options, create_cloud_dagit_client_options
from ...errors import raise_http_error
from ...headers.impl import get_dagster_cloud_api_headers


@click.group(name="saml")
def saml_cli():
    """Customize your SAML settings."""


@saml_cli.command(name="upload-identity-provider-metadata")
@add_options(create_cloud_dagit_client_options)
@click.argument("metadata_file", type=click.File("r"), metavar="METADATA_FILE_PATH")
def upload_identity_provider_metadata_command(metadata_file, url: str, api_token: str):
    """Uploads your identity provider's metadata to enable SAML Single sign-on (SSO)."""

    response = requests.post(
        url=f"{url}/upload_idp_metadata",
        headers=get_dagster_cloud_api_headers(api_token),
        files={"metadata.xml": metadata_file},
    )

    raise_http_error(response)

    click.echo("The identity provider metadata was successfully uploaded.")
