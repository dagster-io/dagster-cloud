from typer import Argument, Context, Option, Typer

from .. import gql, ui
from typing import Optional
from ..config_utils import (
    DagsterCloudCliConfig,
    available_deployment_names,
    dagster_cloud_options,
    read_config,
    write_config,
)

app = Typer()


@app.command()
@dagster_cloud_options()
def set_deployment(
    ctx: Context, deployment: str = Argument(..., autocompletion=available_deployment_names)
):
    deployments = available_deployment_names(ctx=ctx)
    if not deployment or not deployment in deployments:
        raise ui.error(f"Deployment {ui.as_code(deployment)} not found")

    config = read_config()
    new_config = config._replace(default_deployment=deployment)
    write_config(new_config)

    ui.print(f"Default deployment changed to {ui.as_code(deployment)}")


@app.command()
def view(
    show_token: bool = Option(
        False, "--show-token", "-s", help="Whether to display the user token in plaintext."
    )
):
    config = read_config()
    if not show_token and config.user_token:
        config = config._replace(user_token=ui.censor_token(config.user_token))
    config_to_display = {k: v for k, v in config._asdict().items() if v is not None}
    ui.print_yaml(config_to_display)


app_configure = Typer()


@app_configure.command()
@dagster_cloud_options(allow_empty=True)
def configure(organization: str, deployment: str, api_token: str):
    new_org = ui.input("Dagster Cloud organization:", default=organization or "") or None

    new_api_token = ui.password_input("Dagster Cloud user token:", default=api_token or "") or None

    # Attempt to fetch deployment names from server, fallback to a text input upon failure
    deployment_names = []
    if new_org and new_api_token:
        try:
            deployments = gql.fetch_deployments(
                gql.graphql_client_from_url(gql.url_from_config(new_org), new_api_token)
            )
            deployment_names = [deployment["deploymentName"] for deployment in deployments]
        except:
            ui.warn(
                "Could not fetch deployment names from server - organization or user token may be set incorrectly."
            )

    new_deployment: Optional[str] = None
    if deployment_names:
        options = ["None"] + deployment_names
        new_deployment = ui.list_input(
            "Default deployment:", choices=options, default=deployment or "None"
        )
        if new_deployment == "None":
            new_deployment = None
    else:
        new_deployment = ui.input("Default deployment:", default=deployment or "") or None

    write_config(
        DagsterCloudCliConfig(
            organization=new_org, default_deployment=new_deployment, user_token=new_api_token
        )
    )
