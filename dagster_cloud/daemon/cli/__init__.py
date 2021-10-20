import click

from .daemon import run_command


def create_agent_cli_group():
    group = click.Group(name="agent")
    group.add_command(run_command)
    return group


agent_cli = create_agent_cli_group()
