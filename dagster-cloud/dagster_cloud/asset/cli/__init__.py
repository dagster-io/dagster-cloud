import json
import time
from typing import Any

import typer
from typer import Typer

from dagster_cloud_cli import gql, ui
from dagster_cloud_cli.config_utils import dagster_cloud_options

app = Typer(help="Commands for working with Dagster Cloud assets.")

# Dagster's internal synthetic job name for asset-backed jobs
_ASSET_JOB_NAME = "__ASSET_JOB"

# Default repository name for single-repository code locations
_SINGLETON_REPOSITORY_NAME = "__repository__"


@app.command(name="materialize")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def materialize(
    api_token: str,
    url: str,
    deployment: str | None,
    select: str = typer.Option(
        ...,
        "--select",
        "-s",
        help=(
            "Comma-separated asset keys to materialize. "
            "Use '/' to separate path components within a key. "
            "Examples: 'my_asset', 'asset1,asset2', 'prefix/asset1,prefix/asset2'"
        ),
    ),
    location: str = typer.Option(
        ...,
        "-l",
        "--location",
        help="Code location name containing the assets.",
    ),
    repository: str = typer.Option(
        None,
        "-r",
        "--repository",
        help=(
            "Repository name within the code location. "
            "Defaults to the singleton repository if only one is defined."
        ),
    ),
    partition: str | None = typer.Option(
        None,
        "--partition",
        "-p",
        help=(
            "Partition key to materialize (e.g. '2024-01-15'). "
            "Only applies to partitioned assets."
        ),
    ),
    tags: str = typer.Option(
        None,
        "--tags",
        help='JSON dict of additional run tags (e.g. \'{"team": "data"}\').',
    ),
    wait: bool = typer.Option(
        False,
        "-w",
        "--wait",
        help="Block until the run finishes and exit with a non-zero code on failure.",
    ),
    interval: int = typer.Option(
        30,
        "-i",
        "--interval",
        help="Polling interval in seconds when --wait is used.",
    ),
):
    """Trigger asset materialization in a Dagster Cloud deployment.

    \b
    Examples:
      dagster-cloud asset materialize -l my_location --select my_asset
      dagster-cloud asset materialize -l my_location --select asset1,asset2
      dagster-cloud asset materialize -l my_location --select prefix/asset1
      dagster-cloud asset materialize -l my_location --select my_asset --partition 2024-01-15
      dagster-cloud asset materialize -l my_location --select my_asset --wait
    """
    loaded_tags: dict[str, Any] = {}
    if tags:
        try:
            loaded_tags = json.loads(tags)
            if not isinstance(loaded_tags, dict):
                raise ValueError("Tags must be a JSON object.")
        except Exception as e:
            raise ui.error(f"Invalid --tags value: {e}")

    if partition:
        loaded_tags["dagster/partition"] = partition

    repo_name = repository or _SINGLETON_REPOSITORY_NAME

    asset_keys = [k.strip() for k in select.split(",") if k.strip()]
    if not asset_keys:
        raise ui.error("--select requires at least one asset key.")

    with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
        run_id = gql.launch_run(
            client,
            location_name=location,
            repo_name=repo_name,
            job_name=_ASSET_JOB_NAME,
            tags=loaded_tags,
            config={},
            asset_keys=asset_keys,
        )

    if wait:
        ui.print(f"Run {run_id} launched. Waiting for completion...")
        status = None
        with gql.graphql_client_from_url(url, api_token, deployment_name=deployment) as client:
            while True:
                time.sleep(interval)
                try:
                    status = gql.run_status(client, run_id)
                except Exception as e:
                    raise ui.error(f"Failed to poll status for run {run_id}: {e}")

                if status in ("SUCCESS", "FAILURE", "CANCELED"):
                    break
                ui.print(f"  Status: {status}...")

        if status == "SUCCESS":
            ui.print(f"Run {run_id} completed successfully.")
        else:
            raise ui.error(
                f"Run {run_id} ended with status '{status}'. "
                "Check the Dagster Cloud UI for details."
            )
    else:
        ui.print(run_id)
