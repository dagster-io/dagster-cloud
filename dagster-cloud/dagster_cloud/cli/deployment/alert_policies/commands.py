from pathlib import Path
from typing import Sequence

import yaml
from typer import Option, Typer

from ... import gql, ui
from ...config_utils import dagster_cloud_options
from .config_schema import process_alert_policies_config

DEFAULT_ALERT_POLICIES_YAML_FILENAME = "alert_policies.yaml"

app = Typer(help="Interact with your alert policies.")


def _config_to_alert_policy_inputs(alert_policies_config) -> Sequence[dict]:
    alert_policy_inputs = []
    for alert_policy_config in alert_policies_config["alert_policies"]:
        if alert_policy_config["notification_service"].get("email"):
            notification_service = {
                "email": {
                    "emailAddresses": alert_policy_config["notification_service"]["email"][
                        "email_addresses"
                    ],
                }
            }
        elif alert_policy_config["notification_service"].get("slack"):
            notification_service = {
                "slack": {
                    "slackWorkspaceName": alert_policy_config["notification_service"]["slack"][
                        "slack_workspace_name"
                    ],
                    "slackChannelName": alert_policy_config["notification_service"]["slack"][
                        "slack_channel_name"
                    ],
                },
            }

        alert_policy_input = {
            "name": alert_policy_config["name"],
            "description": alert_policy_config["description"],
            "tags": alert_policy_config["tags"],
            "eventTypes": alert_policy_config["event_types"],
            "enabled": alert_policy_config.get("enabled", True),
            **notification_service,
        }

        alert_policy_inputs.append(alert_policy_input)

    return alert_policy_inputs


@app.command(name="list")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def list_command(
    api_token: str,
    url: str,
):
    """List your alert policies."""
    client = gql.graphql_client_from_url(url, api_token)

    alert_policies_response = gql.get_alert_policies(client)

    ui.print_yaml(alert_policies_response)


@app.command(name="sync")
@dagster_cloud_options(allow_empty=True, requires_url=True)
def sync_command(
    api_token: str,
    url: str,
    alert_policies_file: Path = Option(
        DEFAULT_ALERT_POLICIES_YAML_FILENAME,
        "--alert-policies",
        "-a",
        exists=True,
        help="Path to alert policies file.",
    ),
):
    """Sync your YAML configured alert policies to Dagster Cloud."""
    client = gql.graphql_client_from_url(url, api_token)

    with open(str(alert_policies_file), "r") as f:
        config = yaml.load(f.read(), Loader=yaml.SafeLoader)
        process_alert_policies_config(config)

    alert_policy_inputs = _config_to_alert_policy_inputs(config)

    try:
        alert_policies = gql.reconcile_alert_policies(client, alert_policy_inputs)

        ui.print(f"Synced alert policies: {', '.join(alert_policies)}")
    except Exception as e:
        ui.error(str(e))
