from collections import defaultdict
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    Optional,
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
from dagster_dbt import DbtCliInvocation
from dagster_dbt.version import __version__ as dagster_dbt_version
from packaging import version

from ..insights_utils import extract_asset_info_from_event
from .bigquery_utils import build_bigquery_cost_metadata, marker_asset_key_for_job

if TYPE_CHECKING:
    from dbt.adapters.base.impl import BaseAdapter
    from google.cloud import bigquery

OPAQUE_ID_SQL_SIGIL = "bigquery_dagster_dbt_v1_opaque_id"
DEFAULT_BQ_REGION = "region-us"
MIN_DAGSTER_DBT_VERSION = "1.7.0"


@dataclass
class BigQueryCostInfo:
    asset_key: AssetKey
    partition: Optional[str]
    job_id: Optional[str]
    slots_ms: int
    bytes_billed: int

    @property
    def asset_partition_key(self) -> str:
        return (
            f"{self.asset_key.to_string()}:{self.partition}"
            if self.partition
            else self.asset_key.to_string()
        )


def dbt_with_bigquery_insights(
    context: Union[OpExecutionContext, AssetExecutionContext],
    dbt_cli_invocation: DbtCliInvocation,
    dagster_events: Optional[
        Iterable[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult]]
    ] = None,
    skip_config_check=False,
    record_observation_usage: bool = True,
) -> Iterator[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult]]:
    """Wraps a dagster-dbt invocation to associate each BigQuery query with the produced
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
            yield from dbt_with_bigquery_insights(context, dbt_cli_invocation)
    """
    if not skip_config_check:
        adapter_type = dbt_cli_invocation.manifest["metadata"]["adapter_type"]
        if adapter_type != "bigquery":
            raise RuntimeError(
                f"The 'bigquery' adapter must be used but instead found '{adapter_type}'"
            )
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

    if dagster_events is None:
        dagster_events = dbt_cli_invocation.stream()

    asset_info_by_unique_id = {}
    for dagster_event in dagster_events:
        if isinstance(
            dagster_event, (AssetMaterialization, AssetObservation, Output, AssetCheckResult)
        ):
            unique_id = dagster_event.metadata["unique_id"].value
            asset_key, partition = extract_asset_info_from_event(
                context, dagster_event, record_observation_usage
            )
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)
            asset_info_by_unique_id[unique_id] = (asset_key, partition)

        yield dagster_event

    marker_asset_key = marker_asset_key_for_job(context.job_def)
    run_results_json = dbt_cli_invocation.get_artifact("run_results.json")
    invocation_id = run_results_json["metadata"]["invocation_id"]

    # backcompat-proof in case the invocation does not have an instantiated adapter on it
    adapter: Optional["BaseAdapter"] = getattr(dbt_cli_invocation, "adapter", None)
    if not adapter:
        if version.parse(dagster_dbt_version) < version.parse(MIN_DAGSTER_DBT_VERSION):
            upgrade_message = f" Extracting cost information requires dagster_dbt>={MIN_DAGSTER_DBT_VERSION} (found {dagster_dbt_version}). "
        else:
            upgrade_message = ""

        context.log.error(
            "Could not find a BigQuery adapter on the dbt CLI invocation. Skipping cost analysis."
            + upgrade_message
        )
        return

    cost_by_asset = defaultdict(list)
    try:
        with adapter.connection_named("dagster_insights:bigquery_cost"):
            client: "bigquery.Client" = adapter.connections.get_thread_connection().handle
            dataset = client.get_dataset(adapter.config.credentials.schema)
            region = f"region-{dataset.location.lower()}" if dataset.location else DEFAULT_BQ_REGION
            query_result = client.query(
                rf"""
                    SELECT
                    job_id,
                    regexp_extract(query, r"{OPAQUE_ID_SQL_SIGIL}\[\[\[(.*?):{invocation_id}\]\]\]") as unique_id,
                    total_bytes_billed AS bytes_billed,
                    total_slot_ms AS slots_ms
                    FROM `{dataset.project}`.`{region}`.INFORMATION_SCHEMA.JOBS
                    WHERE query like '%{invocation_id}%'
                """
            )
            for row in query_result:
                if not row.unique_id:
                    continue
                asset_key, partition = asset_info_by_unique_id.get(
                    row.unique_id, (marker_asset_key, None)
                )
                if row.bytes_billed or row.slots_ms:
                    cost_info = BigQueryCostInfo(
                        asset_key, partition, row.job_id, row.slots_ms, row.bytes_billed
                    )
                    cost_by_asset[cost_info.asset_partition_key].append(cost_info)
    except:
        context.log.exception("Could not query information_schema for BigQuery cost information")
        return

    for cost_info_list in cost_by_asset.values():
        bytes_billed = sum(item.bytes_billed for item in cost_info_list)
        slots_ms = sum(item.slots_ms for item in cost_info_list)
        job_ids = [item.job_id for item in cost_info_list]
        asset_key = cost_info_list[0].asset_key
        partition = cost_info_list[0].partition
        context.log_event(
            AssetObservation(
                asset_key=asset_key,
                partition=partition,
                metadata=build_bigquery_cost_metadata(job_ids, bytes_billed, slots_ms),
            )
        )
