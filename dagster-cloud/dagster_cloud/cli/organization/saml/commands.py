from pathlib import Path

import requests
from typer import Argument, Option, Typer

from ....errors import raise_http_error
from ....headers.impl import get_dagster_cloud_api_headers
from ... import gql, ui
from ...config_utils import dagster_cloud_options

app = Typer(help="Customize your SAML settings.")


@app.command(name="upload-identity-provider-metadata")
@dagster_cloud_options(allow_empty=True)
def upload_identity_provider_metadata_command(
    api_token: str,
    organization: str,
    url: str = Option(
        None,
        "--url",
        help="[DEPRECATED] Your Dagster Cloud url, in the form of 'https://{ORGANIZATION_NAME}.dagster.cloud'.",
        hidden=True,
    ),
    metadata_file: Path = Argument(..., readable=True, metavar="METADATA_FILE_PATH"),
):
    """Upload your identity provider's metadata to enable SAML Single sign-on (SSO)."""

    if not url and not organization:
        raise ui.error("Must provide either organization name or URL.")

    if not url:
        url = gql.url_from_config(organization=organization)

    with open(metadata_file, "r", encoding="utf8") as f:
        response = requests.post(
            url=f"{url}/upload_idp_metadata",
            headers=get_dagster_cloud_api_headers(api_token),
            files={"metadata.xml": f},
        )

    raise_http_error(response)

    ui.print("The identity provider metadata was successfully uploaded.")
