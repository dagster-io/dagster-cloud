from dagster._core.storage.tags import HIDDEN_TAG_PREFIX, SYSTEM_TAG_PREFIX

CLOUD_SYSTEM_TAG_PREFIX = "dagster-cloud/"

ALERT_EMAILS_TAG = f"{CLOUD_SYSTEM_TAG_PREFIX}alert_emails"

PEX_METADATA_TAG = f"{HIDDEN_TAG_PREFIX}pex_metadata"
PEX_TAG_TAG = f"{SYSTEM_TAG_PREFIX}pex_tag"
