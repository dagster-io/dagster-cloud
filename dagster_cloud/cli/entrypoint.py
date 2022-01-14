from typer import Typer

from ..agent.cli import app as agent_app
from ..workspace.cli import app as workspace_app
from .commands.config import app as config_app
from .commands.config import app_configure as configure_app
from .deployment import app as deployment_app
from .organization import app as organization_app
from .organization import legacy_settings_app


def _import_commands(parent: Typer, child: Typer) -> None:
    """
    Copies the commands from one Typer app to another.
    Equivalent of `add_typer` but doesn't add a subcommand.
    """
    for command in child.registered_commands:
        parent.registered_commands.append(command)


app = Typer(help="CLI tools for working with Dagster Cloud.", no_args_is_help=True)
app.add_typer(agent_app, name="agent")
app.add_typer(workspace_app, name="workspace")
app.add_typer(organization_app, name="organization")
app.add_typer(deployment_app, name="deployment")
# Deprecated in favor of organization
app.add_typer(legacy_settings_app, name="settings", hidden=True)
app.add_typer(config_app, name="config")
_import_commands(app, configure_app)

if __name__ == "__main__":
    app()
