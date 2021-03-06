from dagster import (
    Array,
    BoolSource,
    Field,
    IntSource,
    Noneable,
    Permissive,
    ScalarUnion,
    StringSource,
)
from dagster_cloud_cli.core.graphql_client import DEFAULT_RETRIES, DEFAULT_TIMEOUT


def dagster_cloud_api_config():
    return {
        "url": Field(StringSource, is_required=False),
        "agent_token": Field(StringSource, is_required=True),
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
        # Handle requests for a single non-branch deployment
        "deployment": Field(
            ScalarUnion(scalar_type=str, non_scalar_schema=Array(str)), is_required=False
        ),
        # Handle requests for multiple non-branch deployments
        "deployments": Field(Array(StringSource), is_required=False),
        # Handle requests for all branch deployments (can be combined with `deployment`)`
        "branch_deployments": Field(
            ScalarUnion(
                scalar_type=bool,
                non_scalar_schema={
                    "enabled": Field(BoolSource, default_value=True, is_required=False),
                    "ttl_seconds": Field(
                        IntSource,
                        is_required=False,
                        default_value=3600,
                        description="How long branch deployments should leave a server running once it has been launched.",
                    ),
                },
            ),
            default_value=False,
        ),
        "all_serverless_deployments": Field(BoolSource, default_value=False, is_required=False),
    }
