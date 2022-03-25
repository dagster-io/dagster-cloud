from dagster import check
from dagster.config import Enum, EnumValue, Field, Selector, Shape
from dagster.config.config_type import Array
from dagster.config.validate import process_config


def process_alert_policies_config(alert_policies_config):
    validation = process_config(ALERT_POLICIES_SCHEMA, alert_policies_config)

    check.invariant(
        validation.success,
        ", ".join([error.message for error in validation.errors]),
    )


ALERT_POLICY_SCHEMA = Shape(
    fields={
        "name": Field(
            config=str,
            is_required=True,
            description="Alert policy name.",
        ),
        "description": Field(
            config=str,
            default_value="",
            description="Description of alert policy",
        ),
        "tags": Field(
            config=Array(
                Shape(
                    fields={
                        "key": Field(
                            config=str,
                            is_required=True,
                            description="Specify a tag key.",
                        ),
                        "value": Field(
                            config=str,
                            is_required=True,
                            description="Specify a tag value.",
                        ),
                    },
                    description="A tag key-value pair.",
                )
            ),
            description="The alert policy will apply to code artifacts that have all the specified tags.",
        ),
        "event_types": Field(
            config=Array(
                Enum(
                    name="AlertPolicyEventType",
                    enum_values=[EnumValue("JOB_FAILURE"), EnumValue("JOB_SUCCESS")],
                )
            ),
            description="The selected system event types that will trigger the alert policy.",
        ),
        "notification_service": Field(
            Selector(
                fields={
                    "email": Field(
                        config=Shape(
                            fields={
                                "email_addresses": Field(
                                    config=Array(str),
                                    is_required=True,
                                    description="Email addresses to send alerts to.",
                                )
                            }
                        ),
                        description="Details to customize email notifications for this alert policy.",
                    ),
                    "slack": Field(
                        config=Shape(
                            fields={
                                "slack_workspace_name": Field(
                                    config=str,
                                    is_required=True,
                                    description="The name of your slack workspace.",
                                ),
                                "slack_channel_name": Field(
                                    config=str,
                                    is_required=True,
                                    description="The name of the slack channel in which to post alerts.",
                                ),
                            }
                        )
                    ),
                }
            ),
            is_required=True,
            description="Configure how the alert policy should send a notification.",
        ),
        "enabled": Field(
            config=bool,
            default_value=True,
            description="Whether the alert policy is active or not.",
        ),
    },
    description="Details to customize an alert policy in Dagster Cloud.",
)

ALERT_POLICIES_SCHEMA = Shape(
    fields={
        "alert_policies": Array(ALERT_POLICY_SCHEMA),
    }
)
