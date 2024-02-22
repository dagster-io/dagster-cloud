from typing import Optional, Tuple, Union

import dagster._check as check
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterInvariantViolationError,
    OpExecutionContext,
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
