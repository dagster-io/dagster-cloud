from pathlib import Path

import requests
from typer import Argument, Typer

from ...cli import ui
from ...cli.config_utils import dagster_cloud_options
from ...errors import raise_http_error
from ...headers.impl import get_dagster_cloud_api_headers

app = Typer(help="Customize your SAML settings.")


@app.command(name="upload-identity-provider-metadata")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def upload_identity_provider_metadata_command(
    url: str,
    api_token: str,
    metadata_file: Path = Argument(..., readable=True, metavar="METADATA_FILE_PATH"),
):
    """Uploads your identity provider's metadata to enable SAML Single sign-on (SSO)."""

    with open(metadata_file, "r") as f:
        response = requests.post(
            url=f"{url}/upload_idp_metadata",
            headers=get_dagster_cloud_api_headers(api_token),
            files={"metadata.xml": f},
        )

    raise_http_error(response)

    ui.print("The identity provider metadata was successfully uploaded.")
