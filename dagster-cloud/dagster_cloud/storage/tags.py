from dagster._core.storage.tags import HIDDEN_TAG_PREFIX, SYSTEM_TAG_PREFIX

CLOUD_SYSTEM_TAG_PREFIX = "dagster-cloud/"

PEX_METADATA_TAG = f"{HIDDEN_TAG_PREFIX}pex_metadata"
PEX_TAG_TAG = f"{SYSTEM_TAG_PREFIX}pex_tag"
IGNORE_ALERTS_TAG = f"{CLOUD_SYSTEM_TAG_PREFIX}ignore-alerts"


def get_policy_names_from_tag_value(policies_str):
    """From a comma-delineated string (whitespace allowed), retrieve alert policy names."""
    return [policy_name.strip() for policy_name in policies_str.split(",")]
