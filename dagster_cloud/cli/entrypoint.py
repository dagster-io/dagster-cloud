from dagster_cloud.agent.cli import app as agent_app
from dagster_cloud.settings import app as settings_app
from dagster_cloud.workspace.cli import app as workspace_app
from typer import Typer

from .commands.config import app as config_app
from .commands.config import app_configure as configure_app


def _import_commands(parent: Typer, child: Typer) -> None:
    """
    Copies the commands from one Typer app to another.
    Equivalent of `add_typer` but doesn't add a subcommand.
    """
    for command in child.registered_commands:
        parent.registered_commands.append(command)


app = Typer(help="CLI tools for working with Dagster Cloud.")
app.add_typer(agent_app, name="agent")
app.add_typer(workspace_app, name="workspace")
app.add_typer(settings_app, name="settings")
app.add_typer(config_app, name="config")
_import_commands(app, configure_app)

if __name__ == "__main__":
    app()
