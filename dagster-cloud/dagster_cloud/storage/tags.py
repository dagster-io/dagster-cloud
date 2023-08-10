from dagster._core.storage.tags import HIDDEN_TAG_PREFIX, SYSTEM_TAG_PREFIX

CLOUD_SYSTEM_TAG_PREFIX = "dagster-cloud/"

PEX_METADATA_TAG = f"{HIDDEN_TAG_PREFIX}pex_metadata"
PEX_TAG_TAG = f"{SYSTEM_TAG_PREFIX}pex_tag"
