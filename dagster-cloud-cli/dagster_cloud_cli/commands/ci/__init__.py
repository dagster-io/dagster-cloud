# CI/CD agnostic commands that work with the current CI/CD system

import json
import logging
import os
import pathlib
import shutil
import sys
from collections import Counter
from enum import Enum
from typing import Any, Dict, List, Optional, cast

import typer
from typer import Typer

from dagster_cloud_cli import docker_utils, gql, pex_utils, ui
from dagster_cloud_cli.commands.ci import utils
from dagster_cloud_cli.commands.workspace import wait_for_load
from dagster_cloud_cli.config_utils import (
    AGENT_HEARTBEAT_TIMEOUT_OPTION,
    DAGSTER_ENV_OPTION,
    LOCATION_LOAD_TIMEOUT_OPTION,
    ORGANIZATION_OPTION,
    TOKEN_ENV_VAR_NAME,
    URL_ENV_VAR_NAME,
    dagster_cloud_options,
    get_location_document,
    get_org_url,
)
from dagster_cloud_cli.core import pex_builder, pydantic_yaml
from dagster_cloud_cli.core.artifacts import (
    download_organization_artifact,
    upload_organization_artifact,
)
from dagster_cloud_cli.core.pex_builder import (
    code_location,
    deps,
    github_context,
    gitlab_context,
    parse_workspace,
)
from dagster_cloud_cli.types import CliEventTags, CliEventType

from .. import metrics
from . import checks, report, state

app = Typer(hidden=True, help="CI/CD agnostic commands")


@app.command(help="Print json information about current CI/CD environment")
def inspect(project_dir: str):
    project_dir = os.path.abspath(project_dir)
    source = metrics.get_source()
    info = {"source": str(source), "project-dir": project_dir}
    if source == CliEventTags.source.github:
        info.update(load_github_info(project_dir))
    print(json.dumps(info))


def load_github_info(project_dir: str) -> Dict[str, Any]:
    event = github_context.get_github_event(project_dir)
    return {
        "git-url": event.commit_url,
        "commit-hash": event.github_sha,
    }


@app.command(
    help=(
        "Print the branch deployment name (or nothing) for the current context. Creates a new"
        " branch deployment if necessary. Requires DAGSTER_CLOUD_ORGANIZATION and"
        " DAGSTER_CLOUD_API_TOKEN environment variables."
    )
)
def branch_deployment(
    project_dir: str,
    organization: Optional[str] = ORGANIZATION_OPTION,
    dagster_env: Optional[str] = DAGSTER_ENV_OPTION,
    mark_closed: bool = False,
):
    try:
        if organization:
            url = get_org_url(organization, dagster_env)
        else:
            url = os.environ[URL_ENV_VAR_NAME]
        print(get_deployment_from_context(url, project_dir, mark_closed))
    except ValueError as err:
        logging.error(
            f"cannot determine branch deployment: {err}",
        )
        sys.exit(1)


def get_deployment_from_context(url, project_dir: str, mark_closed=False) -> str:
    source = metrics.get_source()
    if source == CliEventTags.source.github:
        event = github_context.get_github_event(project_dir)
        api_token = os.environ[TOKEN_ENV_VAR_NAME]
        deployment_name = code_location.create_or_update_branch_deployment_from_github_context(
            url, api_token, event, mark_closed
        )
        if not deployment_name:
            raise ValueError(
                f"could not determine branch deployment for PR {event.pull_request_id}"
            )

        return deployment_name
    elif source == CliEventTags.source.gitlab:
        event = gitlab_context.get_gitlab_event(project_dir)
        if not event.merge_request_iid:
            raise ValueError("no merge request id")
        with gql.graphql_client_from_url(url, os.environ[TOKEN_ENV_VAR_NAME]) as client:
            deployment_name = gql.create_or_update_branch_deployment(
                client,
                repo_name=event.project_name,
                branch_name=event.branch_name,
                commit_hash=event.commit_sha,
                timestamp=float(event.git_metadata.timestamp),
                branch_url=event.branch_url,
                pull_request_url=event.merge_request_url,
                pull_request_status=("CLOSED" if mark_closed else "OPEN"),
                pull_request_number=event.merge_request_iid,
                author_name=event.git_metadata.name,
                author_email=event.git_metadata.email,
                commit_message=event.git_metadata.message,
            )
            logging.info(
                "Got branch deployment %r for branch %r",
                deployment_name,
                event.branch_name,
            )
            return deployment_name

    else:
        raise ValueError(f"unsupported for {source}")


@app.command(help="Validate configuration")
def check(
    organization: Optional[str] = ORGANIZATION_OPTION,
    dagster_env: Optional[str] = DAGSTER_ENV_OPTION,
    project_dir: str = typer.Option("."),
    dagster_cloud_yaml_path: str = "dagster_cloud.yaml",
    dagster_cloud_yaml_check: checks.Check = typer.Option("error"),
    dagster_cloud_connect_check: checks.Check = typer.Option("error"),
):
    project_path = pathlib.Path(project_dir)

    verdicts = []
    if dagster_cloud_yaml_check != checks.Check.skip:
        yaml_path = project_path / dagster_cloud_yaml_path
        result = checks.check_dagster_cloud_yaml(yaml_path)
        verdicts.append(
            checks.handle_result(
                result,
                dagster_cloud_yaml_check,
                prefix_message="[dagster_cloud.yaml] ",
                success_message="Checked OK",
                failure_message=(
                    "Invalid dagster_cloud.yaml, please see"
                    " https://docs.dagster.io/dagster-cloud/managing-deployments/dagster-cloud-yaml"
                ),
            )
        )

    if dagster_cloud_connect_check != checks.Check.skip:
        if not organization:
            raise ui.error(
                "DAGSTER_CLOUD_ORGANIZATION or --organization required for connection check."
            )
        url = get_org_url(organization, dagster_env)
        result = checks.check_connect_dagster_cloud(url)
        verdicts.append(
            checks.handle_result(
                result,
                dagster_cloud_connect_check,
                prefix_message="[dagster.cloud connection] ",
                success_message="Able to connect to dagster.cloud",
                failure_message="Unable to connect to dagster.cloud",
            )
        )

    verdict_counts = Counter(verdicts)
    ui.print(f"Passed: {verdict_counts[checks.Verdict.passed]}")
    if verdict_counts[checks.Verdict.warning]:
        ui.print(f"Warnings: {verdict_counts[checks.Verdict.warning]}")
    if verdict_counts[checks.Verdict.failed]:
        ui.print(ui.red(f"Failed: {verdict_counts[checks.Verdict.failed]}"))
        sys.exit(1)


STATEDIR_OPTION = typer.Option(..., envvar="DAGSTER_BUILD_STATEDIR")


@app.command(help="Initialize a build session")
@dagster_cloud_options(allow_empty=False, allow_empty_deployment=True, requires_url=False)
def init(
    organization: str,
    deployment: Optional[str],
    dagster_env: Optional[str] = DAGSTER_ENV_OPTION,
    project_dir: str = typer.Option("."),
    dagster_cloud_yaml_path: str = "dagster_cloud.yaml",
    statedir: str = STATEDIR_OPTION,
    clean_statedir: bool = typer.Option(True, help="Delete any existing files in statedir"),
    location_name: List[str] = typer.Option([]),
    git_url: Optional[str] = None,
    commit_hash: Optional[str] = None,
    status_url: Optional[str] = None,
):
    yaml_path = pathlib.Path(project_dir) / dagster_cloud_yaml_path
    if not yaml_path.exists():
        raise ui.error(
            f"Dagster Cloud yaml file not found at specified path {yaml_path.resolve()}."
        )
    locations_def = pydantic_yaml.load_dagster_cloud_yaml(yaml_path.read_text())
    locations = locations_def.locations
    if location_name:
        selected_locations = set(location_name)
        unknown = selected_locations - set(location.location_name for location in locations)
        if unknown:
            raise ui.error(f"Locations not found in {dagster_cloud_yaml_path}: {unknown}")
        locations = [
            location for location in locations if location.location_name in selected_locations
        ]
    url = get_org_url(organization, dagster_env)
    # Deploy to the branch deployment for the current context. If there is no branch deployment
    # available (eg. if not in a PR) then we fallback to the --deployment flag.

    try:
        branch_deployment = get_deployment_from_context(url, project_dir)
        if deployment:
            ui.print(
                f"Deploying to branch deployment {branch_deployment}, ignoring"
                f" --deployment={deployment}"
            )
        deployment = branch_deployment
        is_branch_deployment = True
    except ValueError as err:
        if deployment:
            ui.print(f"Deploying to {deployment}. No branch deployment ({err}).")
            is_branch_deployment = False
        else:
            raise ui.error(
                f"Cannot determine deployment name in current context ({err}). Please specify"
                " --deployment."
            )

    if clean_statedir and os.path.exists(statedir):
        shutil.rmtree(statedir, ignore_errors=True)
    state_store = state.FileStore(statedir=statedir)

    ui.print(f"Initializing {statedir}")
    for location in locations:
        location_state = state.LocationState(
            url=url,
            deployment_name=deployment,
            location_file=str(yaml_path.absolute()),
            location_name=location.location_name,
            is_branch_deployment=is_branch_deployment,
            build=state.BuildMetadata(
                git_url=git_url, commit_hash=commit_hash, build_config=location.build
            ),
            build_output=None,
            status_url=status_url,
        )
        location_state.add_status_change(state.LocationStatus.pending, "initialized")
        state_store.save(location_state)

    ui.print(
        f"Initialized {statedir} to build and deploying following locations for directory"
        f" {project_dir}:"
    )
    for location in state_store.list_locations():
        ui.print(f"- {location.location_name}")


class StatusOutputFormat(Enum):
    json = "json"
    markdown = "markdown"


@app.command(help="Show status of the current build session")
def status(
    statedir: str = STATEDIR_OPTION,
    output_format: StatusOutputFormat = typer.Option("json", help="Output format for build status"),
):
    state_store = state.FileStore(statedir=statedir)
    location_states = state_store.list_locations()
    if output_format == StatusOutputFormat.json:
        for location in location_states:
            ui.print(location.json())
    elif output_format == StatusOutputFormat.markdown:
        ui.print(report.markdown_report(location_states))


@app.command(help="Update the PR comment with the latest status for branch deployments in Github.")
def notify(
    statedir: str = STATEDIR_OPTION,
    project_dir: str = typer.Option("."),
):
    state_store = state.FileStore(statedir=statedir)
    location_states = state_store.list_locations()

    source = metrics.get_source()
    if source == CliEventTags.source.github:
        event = github_context.get_github_event(project_dir)
        msg = f"Your pull request at commit `{event.github_sha}` is automatically being deployed to Dagster Cloud."
        event.update_pr_comment(
            msg + "\n\n" + report.markdown_report(location_states),
            orig_author="github-actions[bot]",
            orig_text=msg,
        )
    else:
        raise ui.error("'dagster-cloud ci notify' is only available within Github actions.")


@app.command(help="List locations in the current build session")
def locations_list(
    statedir: str = STATEDIR_OPTION,
):
    state_store = state.FileStore(statedir=statedir)
    for location in state_store.list_locations():
        if location.selected:
            ui.print(f"{location.location_name}")
        else:
            ui.print(f"{location.location_name} DESELECTED")


@app.command(help="Mark the specified locations as excluded from the current build session")
def locations_deselect(
    location_names: List[str],
    statedir: str = STATEDIR_OPTION,
):
    state_store = state.FileStore(statedir=statedir)
    state_store.deselect(location_names)
    ui.print("Deselected locations: {locations_names}")


@app.command(help="Mark the specified locations as included in the current build session")
def locations_select(
    location_names: List[str],
    statedir: str = STATEDIR_OPTION,
):
    state_store = state.FileStore(statedir=statedir)
    state_store.select(location_names)
    ui.print("Deselected locations: {locations_names}")


def _get_selected_locations(
    state_store: state.Store, location_name: List[str]
) -> Dict[str, state.LocationState]:
    requested_locations = set(location_name)
    selected = {
        location.location_name: location
        for location in state_store.list_selected_locations()
        if not requested_locations or location.location_name in requested_locations
    }
    unknown_locations = requested_locations - set(selected)
    if unknown_locations:
        raise ui.error(f"Unknown or deselected location names requested: {unknown_locations}")
    return selected


class BuildStrategy(Enum):
    pex = "python-executable"
    docker = "docker"


@app.command(help="Build selected or requested locations")
def build(
    statedir: str = STATEDIR_OPTION,
    location_name: List[str] = typer.Option([]),
    build_directory: Optional[str] = typer.Option(
        None,
        help=(
            "Directory root for building this code location. Read from dagster_cloud.yaml by"
            " default."
        ),
    ),
    build_strategy: BuildStrategy = typer.Option(
        "docker",
        help=(
            "Build strategy used to build code locations. 'docker' builds a docker image."
            " 'python-executable' builds a set of pex files."
        ),
    ),
    docker_image_tag: Optional[str] = typer.Option(
        None, help="Tag for built docker image. Auto-generated by default."
    ),
    docker_base_image: Optional[str] = typer.Option(
        None,
        help="Base image used to build the docker image for --build-strategy=docker.",
    ),
    docker_env: List[str] = typer.Option([], help="Env vars for docker builds."),
    python_version: str = typer.Option(
        "3.8",
        help=(
            "Python version used to build the python-executable; or to determine the default base"
            " image for docker."
        ),
    ),
    pex_build_method: deps.BuildMethod = typer.Option("local"),
    pex_deps_cache_from: Optional[str] = None,
    pex_deps_cache_to: Optional[str] = None,
    pex_base_image_tag: Optional[str] = typer.Option(
        None,
        help="Base image used to run python executable for --build-strategy=python-executable.",
    ),
) -> None:
    if python_version:
        # ensure version is parseable
        pex_builder.util.parse_python_version(python_version)

    if build_strategy == BuildStrategy.pex:
        if docker_base_image or docker_image_tag:
            raise ui.error(
                "--base-image or --image-tag not supported for --build-strategy=python-executable."
            )

    state_store = state.FileStore(statedir=statedir)
    locations = _get_selected_locations(state_store, location_name)
    ui.print("Going to build the following locations:")
    for name in locations:
        ui.print(f"- {name}")

    for name, location_state in locations.items():
        try:
            configured_build_directory = (
                location_state.build.build_config.directory
                if (
                    location_state.build.build_config
                    and location_state.build.build_config.directory
                )
                else None
            )
            if build_directory and configured_build_directory:
                ui.warn(
                    f"Overriding configured build:directory:{configured_build_directory!r} with"
                    f" cmdline provided --build-directory={build_directory!r}"
                )
                location_build_dir = build_directory
            elif (not build_directory) and configured_build_directory:
                location_build_dir = configured_build_directory
            elif build_directory and (not configured_build_directory):
                location_build_dir = build_directory
            else:
                location_build_dir = "."

            url = location_state.url
            api_token = os.environ.get(TOKEN_ENV_VAR_NAME, "")

            if build_strategy == BuildStrategy.docker:
                if not docker_base_image:
                    docker_base_image = f"python:{python_version}-slim"
                location_state.build_output = _build_docker(
                    url=url,
                    api_token=api_token,
                    name=name,
                    location_build_dir=location_build_dir,
                    docker_base_image=docker_base_image,
                    docker_env=docker_env,
                    location_state=location_state,
                )
                state_store.save(location_state)
            elif build_strategy == BuildStrategy.pex:
                location_state.build_output = _build_pex(
                    url=url,
                    api_token=api_token,
                    name=name,
                    location_build_dir=location_build_dir,
                    python_version=python_version,
                    pex_build_method=pex_build_method,
                    pex_deps_cache_from=pex_deps_cache_from,
                    pex_deps_cache_to=pex_deps_cache_to,
                    pex_base_image_tag=pex_base_image_tag,
                    location_state=location_state,
                )
                state_store.save(location_state)
                ui.print(
                    "Built and uploaded python executable"
                    f" {location_state.build_output.pex_tag} for location {name}"
                )
        except:
            location_state.add_status_change(state.LocationStatus.failed, "build failed")
            state_store.save(location_state)
            raise
        else:
            location_state.add_status_change(state.LocationStatus.pending, "build successful")
            state_store.save(location_state)


@metrics.instrument(
    CliEventType.BUILD,
    tags=[CliEventTags.subcommand.dagster_cloud_ci, CliEventTags.server_strategy.docker],
)
# url and api_token are used by the instrument decorator
def _build_docker(
    url: str,
    api_token: str,
    name: str,
    location_build_dir: str,
    docker_base_image: str,
    docker_env: List[str],
    location_state: state.LocationState,
) -> state.DockerBuildOutput:
    name = location_state.location_name
    ui.print(f"Building docker image for location {name} using base image {docker_base_image}")
    docker_utils.verify_docker()
    registry_info = utils.get_registry_info(url)

    docker_image_tag = docker_utils.default_image_tag(
        location_state.deployment_name, name, location_state.build.commit_hash
    )

    retval = docker_utils.build_image(
        location_build_dir,
        docker_image_tag,
        registry_info,
        env_vars=docker_env,
        base_image=docker_base_image,
    )
    if retval != 0:
        raise ui.error(f"Failed to build docker image for location {name}")

    retval = docker_utils.upload_image(docker_image_tag, registry_info)
    if retval != 0:
        raise ui.error(f"Failed to upload docker image for location {name}")

    image = f'{registry_info["registry_url"]}:{docker_image_tag}'
    ui.print(f"Built and uploaded image {image} for location {name}")

    return state.DockerBuildOutput(image=image)


@metrics.instrument(
    CliEventType.BUILD,
    tags=[CliEventTags.subcommand.dagster_cloud_ci, CliEventTags.server_strategy.pex],
)
# url and api_token are used by the instrument decorator
def _build_pex(
    url: str,
    api_token: str,
    name: str,
    location_build_dir: str,
    python_version: str,
    pex_build_method: deps.BuildMethod,
    pex_deps_cache_from: Optional[str],
    pex_deps_cache_to: Optional[str],
    pex_base_image_tag: Optional[str],
    location_state: state.LocationState,
) -> state.PexBuildOutput:
    pex_location = parse_workspace.Location(
        name,
        directory=location_build_dir,
        build_folder=location_build_dir,
        location_file=location_state.location_file,
    )

    location_kwargs = pex_utils.build_upload_pex(
        url=url,
        api_token=api_token,
        location=pex_location,
        build_method=pex_build_method,
        kwargs={
            "python_version": python_version,
            "base_image_tag": pex_base_image_tag,
            "deps_cache_from": pex_deps_cache_from,
            "deps_cache_to": pex_deps_cache_to,
        },
    )
    return state.PexBuildOutput(
        python_version=python_version,
        image=location_kwargs["image"],
        pex_tag=location_kwargs["pex_tag"],
    )


@app.command(help="Update the current build session for an externally built docker image.")
def set_build_output(
    statedir: str = STATEDIR_OPTION,
    location_name: List[str] = typer.Option([]),
    image_tag: str = typer.Option(
        ...,
        help=(
            "Tag for the built docker image. Note the registry must be specified in"
            " dagster_cloud.yaml."
        ),
    ),
) -> None:
    state_store = state.FileStore(statedir=statedir)
    locations = _get_selected_locations(state_store, location_name)
    ui.print("Going to update the following locations:")
    for name in locations:
        ui.print(f"- {name}")

    # validation pass - computes the full image name for all locations
    images = {}
    for name, location_state in locations.items():
        configured_defs = pydantic_yaml.load_dagster_cloud_yaml(
            open(location_state.location_file, encoding="utf-8").read()
        )
        location_defs = [loc for loc in configured_defs.locations if loc.location_name == name]
        if not location_defs:
            raise ui.error(f"Location {name} not found in {location_state.location_file}")
        location_def = location_defs[0]
        registry = location_def.build.registry if location_def.build else None
        if not registry:
            raise ui.error(
                f"No build:registry: defined for location {name} in {location_state.location_file}"
            )

        images[name] = f"{registry}:{image_tag}"

    # save pass - save full image name computed in the previous pass for all locations
    for name, location_state in locations.items():
        # Update and save build state
        location_state.build_output = state.DockerBuildOutput(image=images[name])
        state_store.save(location_state)
        ui.print(f"Recorded image {images[name]} for location {name}")
    ui.print("Use 'ci deploy' to update dagster-cloud.")


@app.command(help="Deploy built code locations to dagster cloud.")
def deploy(
    statedir: str = STATEDIR_OPTION,
    location_name: List[str] = typer.Option([]),
    location_load_timeout: int = LOCATION_LOAD_TIMEOUT_OPTION,
    agent_heartbeat_timeout: int = AGENT_HEARTBEAT_TIMEOUT_OPTION,
):
    state_store = state.FileStore(statedir=statedir)
    locations = _get_selected_locations(state_store, location_name)
    ui.print("Going deploy the following locations:")

    built_locations: List[state.LocationState] = []
    for name, location_state in locations.items():
        if location_state.build_output:
            status = "Ready to deploy"
            built_locations.append(location_state)
        else:
            status = "Not ready to deploy"
        ui.print(f"- {name} [{status}]")

    if len(built_locations) < len(locations):
        raise ui.error(
            "Cannot deploy because locations have not been built. Use 'ci build' to build"
            " locations."
        )

    if not built_locations:
        ui.print("No locations to deploy")
        return

    try:
        _deploy(
            url=built_locations[0].url,
            api_token=os.environ[TOKEN_ENV_VAR_NAME],
            built_locations=built_locations,
            location_load_timeout=location_load_timeout,
            agent_heartbeat_timeout=agent_heartbeat_timeout,
        )
    except:
        # unfortunately we do not know if only a subset of locations failed to deploy
        for location_state in built_locations:
            location_state.add_status_change(state.LocationStatus.failed, "deploy failed")
            state_store.save(location_state)
        raise
    else:
        for location_state in built_locations:
            location_state.add_status_change(state.LocationStatus.success, "deploy successful")
            state_store.save(location_state)

    deployment_url = built_locations[0].url + "/" + built_locations[0].deployment_name
    ui.print(f"View the status of your locations at {deployment_url}/locations.")


@metrics.instrument(CliEventType.DEPLOY, tags=[CliEventTags.subcommand.dagster_cloud_ci])
# url and api_token are used by the instrument decorator
def _deploy(
    *,
    url: str,
    api_token: str,
    built_locations: List[state.LocationState],
    location_load_timeout: int,
    agent_heartbeat_timeout: int,
):
    for location_state in built_locations:
        if not location_state.build_output:  # not necessary but keep type checker happy
            continue

        location_args = {
            "image": location_state.build_output.image,
            "location_file": location_state.location_file,
            "git_url": location_state.build.git_url,
            "commit_hash": location_state.build.commit_hash,
        }
        if location_state.build_output.strategy == "python-executable":
            metrics.instrument_add_tags([CliEventTags.server_strategy.pex])
            location_args["pex_tag"] = location_state.build_output.pex_tag
        else:
            metrics.instrument_add_tags([CliEventTags.server_strategy.docker])

        with utils.client_from_env(location_state.url, location_state.deployment_name) as client:
            location_document = get_location_document(location_state.location_name, location_args)
            gql.add_or_update_code_location(client, location_document)
            ui.print(f"Updated code location {location_state.location_name} in dagster-cloud")

    deployment_url = built_locations[0].url + "/" + built_locations[0].deployment_name
    with utils.client_from_env(
        built_locations[0].url, deployment=built_locations[0].deployment_name
    ) as client:
        wait_for_load(
            client,
            [location.location_name for location in built_locations],
            location_load_timeout=location_load_timeout,
            agent_heartbeat_timeout=agent_heartbeat_timeout,
            url=deployment_url,
        )


dagster_dbt_app = typer.Typer(
    hidden=True,
    help="Dagster Cloud commands for managing the `dagster-dbt` integration.",
    add_completion=False,
)
app.add_typer(dagster_dbt_app, name="dagster-dbt", no_args_is_help=True)

project_app = typer.Typer(
    name="project",
    no_args_is_help=True,
    help="Commands for using a dbt project in Dagster.",
    add_completion=False,
)
dagster_dbt_app.add_typer(project_app, name="project", no_args_is_help=True)


@project_app.command(
    name="manage-state",
    help="""
    This CLI command will handle uploading and downloading artifacts if `state_dir` is specified on
    `DbtProject`.
    """,
)
def manage_state_command(
    statedir: str = STATEDIR_OPTION,
    file: str = typer.Option(),
    source_deployment: str = typer.Option(
        default="prod",
        help="Which deployment should upload its manifest.json.",
    ),
    key_prefix: str = typer.Option(
        default="",
        help="A key prefix for the key the manifest.json is saved with.",
    ),
):
    try:
        from dagster_dbt import DbtProject
    except:
        ui.print(
            "Unable to import dagster_dbt, can not use dbt-prepare-for-deployment when dagster_dbt is not installed."
        )
        return
    from dagster._core.code_pointer import load_python_file
    from dagster._core.definitions.load_assets_from_modules import find_objects_in_module_of_types

    state_store = state.FileStore(statedir=statedir)
    locations = state_store.list_locations()
    if not locations:
        raise ui.error("Unable to determine deployment state.")

    location = locations[0]
    deployment_name = location.deployment_name
    is_branch = location.is_branch_deployment

    contents = load_python_file(file, None)
    for project in find_objects_in_module_of_types(contents, DbtProject):
        project = cast(DbtProject, project)
        if project.state_path:
            download_path = project.state_path.joinpath("manifest.json")
            key = f"{key_prefix}{os.fspath(download_path)}"
            if is_branch:
                ui.print(f"Downloading {source_deployment} manifest for branch deployment.")
                os.makedirs(project.state_path, exist_ok=True)
                download_organization_artifact(key, download_path)
                ui.print("Download complete.")

            elif deployment_name == source_deployment:
                ui.print(f"Uploading {source_deployment} manifest.")
                upload_organization_artifact(key, project.manifest_path)
                ui.print("Upload complete")

        ui.print("Project ready")
