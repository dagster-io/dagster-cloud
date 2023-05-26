import json
from typing import Any, Dict, List, Optional

import typer
from dagster._core.definitions.repository_definition import SINGLETON_REPOSITORY_NAME
from typer import Typer

from ... import gql, ui
from ...config_utils import dagster_cloud_options

app = Typer(help="Commands for working with Dagster Cloud jobs.")


@app.command(name="launch")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def launch(
    api_token: str,
    url: str,
    location: str = typer.Option(..., "-l", "--location", help="Location name in the deployment."),
    job: str = typer.Option(..., "-j", "--job", help="Job name to run."),
    repository: str = typer.Option(
        None,
        "-r",
        "--repository",
        help=(
            "Repository in the specified code location. Required if a repository is defined in the"
            " specified code location."
        ),
    ),
    tags: str = typer.Option(None, "--tags", help="JSON dict of tags to use for this job run."),
    config: str = typer.Option(
        None, "--config-json", help="JSON string of run config to use for this job run"
    ),
    asset_keys: Optional[List[str]] = typer.Option(
        None,
        "--asset-key",
        help=(
            "Asset key to materialize. Can be specified multiple times to materialize multiple"
            " assets."
        ),
    ),
):
    """Launch a run for a job."""
    loaded_tags: Dict[str, Any] = json.loads(tags) if tags else {}
    loaded_config: Dict[str, Any] = json.loads(config) if config else {}

    repository = repository or SINGLETON_REPOSITORY_NAME
    with gql.graphql_client_from_url(url, api_token) as client:
        ui.print(
            gql.launch_run(
                client,
                location,
                repository,
                job,
                loaded_tags,
                loaded_config,
                asset_keys=asset_keys,
            )
        )
