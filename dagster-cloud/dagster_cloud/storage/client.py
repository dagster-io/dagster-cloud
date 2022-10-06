from dagster import (
    Array,
    BoolSource,
    Field,
    IntSource,
    Map,
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
        "retries": Field(IntSource, default_value=DEFAULT_RETRIES),
        "method": Field(StringSource, default_value="POST"),
        "proxies": Field(Map(str, str), is_required=False),
        "agent_label": Field(StringSource, is_required=False),
        # Handle requests for a single non-branch deployment
        "deployment": Field(
            ScalarUnion(scalar_type=str, non_scalar_schema=Array(str)), is_required=False
        ),
        # Handle requests for multiple non-branch deployments
        "deployments": Field(Array(StringSource), is_required=False),
        # Handle requests for all branch deployments (can be combined with `deployment`)`
        "branch_deployments": Field(BoolSource, default_value=False, is_required=False),
        "all_serverless_deployments": Field(BoolSource, default_value=False, is_required=False),
    }
