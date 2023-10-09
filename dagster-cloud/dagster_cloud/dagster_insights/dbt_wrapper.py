from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)
from uuid import uuid4 as uuid

import yaml
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetObservation,
    OpExecutionContext,
    Output,
)

if TYPE_CHECKING:
    from dagster_dbt import DbtCliInvocation

# Metadata key prefix used to tag Snowflake queries with opaque IDs
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_snowflake_opaque_id:"
OPAQUE_ID_SQL_SIGIL = "snowflake_dagster_dbt_v1_opaque_id"


def add_asset_context_to_sql_query(
    context: AssetExecutionContext,
    sql: str,
    comment_factory=lambda comment: f"\n-- {comment}\n",
    opaque_id=None,
):
    if opaque_id is None:
        opaque_id = str(uuid())
    context.add_output_metadata({f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True})
    return sql + comment_factory(f"{OPAQUE_ID_SQL_SIGIL}[[[{opaque_id}]]]")


def dbt_with_snowflake_insights(
    context: Union[OpExecutionContext, AssetExecutionContext],
    dbt_cli_invocation: "DbtCliInvocation",
    dagster_events: Optional[Iterable[Union[Output, AssetObservation, AssetCheckResult]]] = None,
    skip_config_check=False,
) -> Iterator[Union[Output, AssetObservation, AssetCheckResult]]:
    """Wraps a dagster-dbt invocation to associate each Snowflake query with the produced
    asset materializations. This allows the cost of each query to be associated with the asset
    materialization that it produced.

    Args:
        context (AssetExecutionContext): The context of the asset that is being materialized.
        dbt_cli_invocation (DbtCliInvocation): The invocation of the dbt CLI to wrap.
        dagster_events (Optional[Iterable[Union[Output, AssetObservation, AssetCheckResult]]]):
            The events that were produced by the dbt CLI invocation. If not provided, it is assumed
            that the dbt CLI invocation has not yet been run, and it will be run and the events
            will be streamed.
        skip_config_check (bool): If true, skips the check that the dbt project config is set up
            correctly. Defaults to False.

    **Example:**

    .. code-block:: python

        @dbt_assets(manifest=DBT_MANIFEST_PATH)
        def jaffle_shop_dbt_assets(
            context: AssetExecutionContext,
            dbt: DbtCliResource,
        ):
            dbt_cli_invocation = dbt.cli(["build"], context=context)
            yield from dbt_with_snowflake_insights(context, dbt_cli_invocation)
    """
    if not skip_config_check:
        is_snowflake = dbt_cli_invocation.manifest["metadata"]["adapter_type"] == "snowflake"
        dbt_project_config = yaml.safe_load(
            (dbt_cli_invocation.project_dir / "dbt_project.yml").open("r")
        )
        # sanity check that the sigil is present somewhere in the query comment
        query_comment = dbt_project_config.get("query-comment")
        if query_comment is None:
            raise RuntimeError("query-comment is required in dbt_project.yml but it was missing")
        comment = query_comment.get("comment")
        if comment is None:
            raise RuntimeError(
                "query-comment.comment is required in dbt_project.yml but it was missing"
            )
        if OPAQUE_ID_SQL_SIGIL not in comment:
            raise RuntimeError(
                "query-comment.comment in dbt_project.yml must contain the string"
                f" '{OPAQUE_ID_SQL_SIGIL}'. Read the Dagster Insights docs for more info."
            )
        if is_snowflake:
            # snowflake throws away prepended comments
            if not query_comment.get("append"):
                raise RuntimeError(
                    "query-comment.append must be true in dbt_project.yml when using the Snowflake"
                    " adapter"
                )

    if dagster_events is None:
        dagster_events = dbt_cli_invocation.stream()

    asset_and_partition_key_to_unique_id: List[Tuple[AssetKey, Optional[str], Any]] = []
    for dagster_event in dagster_events:
        if isinstance(dagster_event, Output):
            unique_id = dagster_event.metadata["unique_id"].value
            asset_key = context.asset_key_for_output(dagster_event.output_name)
            if context._step_execution_context.has_asset_partitions_for_output(  # noqa: SLF001
                dagster_event.output_name
            ):
                partition_key = context.asset_partition_key_for_output(dagster_event.output_name)
            else:
                partition_key = None
            asset_and_partition_key_to_unique_id.append((asset_key, partition_key, unique_id))

        elif isinstance(dagster_event, AssetObservation):
            unique_id = dagster_event.metadata["unique_id"].value
            asset_key = dagster_event.asset_key
            partition_key = dagster_event.partition
            asset_and_partition_key_to_unique_id.append((asset_key, partition_key, unique_id))

        yield dagster_event

    run_results_json = dbt_cli_invocation.get_artifact("run_results.json")
    invocation_id = run_results_json["metadata"]["invocation_id"]

    for asset_key, partition, unique_id in asset_and_partition_key_to_unique_id:
        # must match the query-comment in dbt_project.yml
        opaque_id = f"{unique_id}:{invocation_id}"
        context.log_event(
            AssetObservation(
                asset_key=asset_key,
                partition=partition,
                metadata={f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True},
            )
        )
