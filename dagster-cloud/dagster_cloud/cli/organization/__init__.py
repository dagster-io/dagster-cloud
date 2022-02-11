from typer import Typer

from .saml import commands as saml_cli

app = Typer(help="Customize your Dagster Cloud organization.")
settings_app = Typer(help="Customize your organization settings.")

app.add_typer(settings_app, name="settings", no_args_is_help=True)
settings_app.add_typer(saml_cli.app, name="saml", no_args_is_help=True)

# Legacy, to support the old `dagster-cloud settings` path
# New command is `dagster-cloud organization
legacy_settings_app = Typer(
    help="[Deprecated, in favor of dagster-cloud organization] Customize your dagster-cloud settings.",
    hidden=True,
)
legacy_settings_app.add_typer(saml_cli.app, name="saml")
