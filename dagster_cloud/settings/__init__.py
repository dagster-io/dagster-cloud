from typer import Typer

from .login import commands as login_cli
from .saml import commands as saml_cli

app = Typer(help="Customize your dagster-cloud settings.")
app.add_typer(login_cli.app, name="login")
app.add_typer(saml_cli.app, name="saml")
