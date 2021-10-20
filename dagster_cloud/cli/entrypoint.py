import click
from dagster_cloud.daemon.cli import agent_cli
from dagster_cloud.settings import settings_cli
from dagster_cloud.workspace.cli import workspace_cli


def create_dagster_cloud_cli():
    commands = {
        "agent": agent_cli,
        "workspace": workspace_cli,
        "settings": settings_cli,
    }

    @click.group(commands=commands)
    def group():
        "CLI tools for working with dagster-cloud."

    return group


cli = create_dagster_cloud_cli()


def main():
    cli(obj={})  # pylint:disable=E1123
