from dagster import Field, IntSource, Noneable, Permissive, StringSource
from dagster_cloud_cli.core.graphql_client import DEFAULT_RETRIES, DEFAULT_TIMEOUT


def dagster_cloud_api_config():
    return {
        "url": Field(StringSource, is_required=False),
        "agent_token": Field(StringSource, is_required=True),
        "deployment": Field(StringSource, is_required=False),
        "headers": Field(Permissive(), default_value={}),
        "cookies": Field(Permissive(), default_value={}),
        "timeout": Field(Noneable(IntSource), default_value=DEFAULT_TIMEOUT),
        "verify": Field(bool, default_value=True),
        # This may be how we want to implement our retry policy, but it may be too restrictive:
        # we may want to put this logic into the storage itself so that we can do some kind of
        # logging
        "retries": Field(IntSource, default_value=DEFAULT_RETRIES),
        "method": Field(StringSource, default_value="POST"),
        "agent_label": Field(StringSource, is_required=False),
    }
