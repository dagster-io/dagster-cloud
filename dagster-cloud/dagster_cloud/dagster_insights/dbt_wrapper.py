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

import yaml
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    OpExecutionContext,
    Output,
)

from .snowflake.snowflake_utils import (
    OPAQUE_ID_SQL_SIGIL,
    build_opaque_id_metadata,
    marker_asset_key_for_job,
)

if TYPE_CHECKING:
    from dagster_dbt import DbtCliInvocation


def _opt_asset_key_for_output(
    context: Union[OpExecutionContext, AssetExecutionContext], output_name: str
) -> Optional[AssetKey]:
    asset_info = context.job_def.asset_layer.asset_info_for_output(
        node_handle=context.op_handle, output_name=output_name
    )
    if asset_info is None:
        return None
    return asset_info.key


def dbt_with_snowflake_insights(
    context: Union[OpExecutionContext, AssetExecutionContext],
    dbt_cli_invocation: "DbtCliInvocation",
    dagster_events: Optional[
        Iterable[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult]]
    ] = None,
    skip_config_check=False,
    record_observation_usage: bool = True,
) -> Iterator[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult]]:
    """Wraps a dagster-dbt invocation to associate each Snowflake query with the produced
    asset materializations. This allows the cost of each query to be associated with the asset
    materialization that it produced.

    If called in the context of an op (rather than an asset), filters out any Output events
    which do not correspond with any output of the op.

    Args:
        context (AssetExecutionContext): The context of the asset that is being materialized.
        dbt_cli_invocation (DbtCliInvocation): The invocation of the dbt CLI to wrap.
        dagster_events (Optional[Iterable[Union[Output, AssetObservation, AssetCheckResult]]]):
            The events that were produced by the dbt CLI invocation. If not provided, it is assumed
            that the dbt CLI invocation has not yet been run, and it will be run and the events
            will be streamed.
        skip_config_check (bool): If true, skips the check that the dbt project config is set up
            correctly. Defaults to False.
        record_observation_usage (bool): If True, associates the usage associated with
            asset observations with that asset. Default is True.

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
        # When calling dbt from an asset context, the dbt CLI invocation will emit Outputs,
        # in the op context it'll be AssetObservations - we can extract the same data from both
        if isinstance(dagster_event, (Output, AssetMaterialization)):
            unique_id = dagster_event.metadata["unique_id"].value

            if isinstance(dagster_event, Output):
                asset_key = _opt_asset_key_for_output(context, dagster_event.output_name)
                partition_key = None
                if asset_key and context._step_execution_context.has_asset_partitions_for_output(  # noqa: SLF001
                    dagster_event.output_name
                ):
                    partition_key = context.asset_partition_key_for_output(
                        dagster_event.output_name
                    )
            else:
                asset_key = dagster_event.asset_key
                partition_key = dagster_event.partition

            if asset_key:
                asset_and_partition_key_to_unique_id.append((asset_key, partition_key, unique_id))
            else:
                # Outputs should only be emitted in the context of an SDA, so this should be a dead
                # code path after https://github.com/dagster-io/dagster/pull/17405/files, but we
                # retain it as a fallback in case users have any old-style custom dbt event conversion logic
                asset_and_partition_key_to_unique_id.append(
                    (marker_asset_key_for_job(context.job_def), None, unique_id)
                )

        elif isinstance(dagster_event, AssetObservation):
            unique_id = dagster_event.metadata["unique_id"].value
            if record_observation_usage:
                asset_key = dagster_event.asset_key
                partition_key = dagster_event.partition
            else:
                # If we don't want to record usage associated with an asset, we still want to
                # record the usage associated with the job, so we use a special sigil for the
                # emitted observation to make sure it doesn't get associated with a real asset key
                asset_key = marker_asset_key_for_job(context.job_def)
                partition_key = None
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
                metadata=build_opaque_id_metadata(opaque_id),
            )
        )
