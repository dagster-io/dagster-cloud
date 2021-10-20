import click

from .saml.commands import saml_cli


def create_settings_cli_group():
    commands = {
        "saml": saml_cli,
    }

    @click.group(commands=commands)
    def group():
        """Customize your dagster-cloud settings."""

    return group


settings_cli = create_settings_cli_group()
