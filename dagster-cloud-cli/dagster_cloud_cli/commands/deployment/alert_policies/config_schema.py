import dagster._check as check
from dagster import Array, Enum, EnumValue, Field, Selector, Shape
from dagster._config import validate_config


def validate_alert_policy_config(alert_policy_config):
    validation = validate_config(ALERT_POLICY_SCHEMA, alert_policy_config)
    return [error.message for error in validation.errors]


def validate_alert_policies_config(alert_policies_config):
    validation = validate_config(ALERT_POLICIES_SCHEMA, alert_policies_config)
    return [error.message for error in validation.errors]


def process_alert_policies_config(alert_policies_config):
    validation = validate_config(ALERT_POLICIES_SCHEMA, alert_policies_config)

    check.invariant(
        validation.success,
        ", ".join([error.message for error in validation.errors]),
    )

    # Validate each individual alert policy
    for alert_policy_config in alert_policies_config["alert_policies"]:
        check.invariant(
            "tags" not in alert_policy_config or len(alert_policy_config["tags"]) > 0,
            (
                "When setting tags for an alert policy, the configuration "
                "must contain at least one tag."
            ),
        )

        email_notification_service = alert_policy_config["notification_service"].get("email")

        # Validate email alerts
        if email_notification_service:
            check.invariant(
                len(email_notification_service["email_addresses"]) > 0,
                (
                    "When creating an alert policy to send email alerts, "
                    "the configuration must contain at least one email address."
                ),
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
            description=(
                "The alert policy will apply to code artifacts that have all the specified tags."
                " When tags are explicitly omitted, this alert policy will apply to all code"
                " artifacts."
            ),
            is_required=False,
        ),
        "event_types": Field(
            config=Array(
                Enum(
                    name="AlertPolicyEventType",
                    enum_values=[
                        EnumValue("JOB_FAILURE", description="Alert on job failure."),
                        EnumValue("JOB_SUCCESS", description="Alert on job success."),
                        EnumValue("TICK_FAILURE", description="Alert on schedule/sensor failure."),
                        EnumValue("AGENT_UNAVAILABLE", description="Alert on agent downtime."),
                        EnumValue(
                            "CODE_LOCATION_ERROR", description="Alert on code location error."
                        ),
                    ],
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
                        description=(
                            "Details to customize email notifications for this alert policy."
                        ),
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
                                    description=(
                                        "The name of the slack channel in which to post alerts."
                                    ),
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
