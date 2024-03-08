from typing import Optional, Tuple, Union

import dagster._check as check
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    DagsterInvariantViolationError,
    OpExecutionContext,
    Output,
)
from dagster._core.errors import DagsterInvalidPropertyError


def get_current_context_and_asset_key() -> (
    Tuple[Union[OpExecutionContext, AssetExecutionContext], Optional[AssetKey]]
):
    asset_key = None
    try:
        context = AssetExecutionContext.get()
        if len(context.assets_def.keys_by_output_name.keys()) == 1:
            asset_key = context.asset_key
    except (DagsterInvalidPropertyError, DagsterInvariantViolationError):
        context = OpExecutionContext.get()

    return check.not_none(context), asset_key


def get_asset_key_for_output(
    context: Union[OpExecutionContext, AssetExecutionContext], output_name: str
) -> Optional[AssetKey]:
    asset_info = context.job_def.asset_layer.asset_info_for_output(
        node_handle=context.op_handle, output_name=output_name
    )
    if asset_info is None:
        return None
    return asset_info.key


def extract_asset_info_from_event(context, dagster_event, record_observation_usage):
    if isinstance(dagster_event, AssetMaterialization):
        return dagster_event.asset_key, dagster_event.partition

    if isinstance(dagster_event, AssetObservation) and record_observation_usage:
        return dagster_event.asset_key, dagster_event.partition

    if isinstance(dagster_event, AssetObservation):
        return None, None

    if isinstance(dagster_event, Output):
        asset_key = get_asset_key_for_output(context, dagster_event.output_name)
        partition_key = None
        if asset_key and context._step_execution_context.has_asset_partitions_for_output(  # noqa: SLF001
            dagster_event.output_name
        ):
            # We associate cost with the first partition key in the case that an output
            # maps to multiple partitions. This is a temporary solution, but partition key
            # is not used in Insights at the moment.
            # TODO: Find a long-term solution for this
            partition_key = next(
                iter(context.asset_partition_keys_for_output(dagster_event.output_name))
            )

        return asset_key, partition_key

    return None, None
