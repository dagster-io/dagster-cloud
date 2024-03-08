from collections import defaultdict
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Optional,
    Union,
)

import dagster._check as check
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    MetadataValue,
    OpExecutionContext,
    Output,
)
from dagster._core.definitions.metadata import MetadataMapping
from google.cloud import bigquery

from ..insights_utils import extract_asset_info_from_event
from .bigquery_utils import build_bigquery_cost_metadata, marker_asset_key_for_job

if TYPE_CHECKING:
    from dagster_dbt import DbtCliInvocation


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


def _extract_metadata_value(value: Optional[MetadataValue], default_value: Any = None) -> Any:
    return value.value if value else default_value


def _extract_bigquery_info_from_metadata(metadata: MetadataMapping):
    job_id = _extract_metadata_value(metadata.get("job_id"))
    bytes_billed = _extract_metadata_value(metadata.get("bytes_billed"), 0)
    slots_ms = _extract_metadata_value(metadata.get("slots_ms"), 0)
    return job_id, bytes_billed, slots_ms


def _asset_partition_key(asset_key: AssetKey, partition_key: Optional[str]) -> str:
    return f"{asset_key.to_string()}:{partition_key}" if partition_key else asset_key.to_string()


def dbt_with_bigquery_insights(
    context: Union[OpExecutionContext, AssetExecutionContext],
    dbt_cli_invocation: "DbtCliInvocation",
    dagster_events: Optional[
        Iterable[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult]]
    ] = None,
    record_observation_usage: bool = True,
    explicitly_query_information_schema: bool = False,
    bigquery_client: Optional[bigquery.Client] = None,
    bigquery_location: Optional[str] = None,
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
    if explicitly_query_information_schema:
        check.inst_param(bigquery_client, "bigquery_client", bigquery.Client)
        check.str_param(bigquery_location, "bigquery_location")

    if dagster_events is None:
        dagster_events = dbt_cli_invocation.stream()

    asset_info_by_job_id = {}
    cost_by_asset = defaultdict(list)
    for dagster_event in dagster_events:
        if isinstance(dagster_event, (AssetMaterialization, AssetObservation, Output)):
            asset_key, partition = extract_asset_info_from_event(
                context, dagster_event, record_observation_usage
            )
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)
            job_id, bytes_billed, slots_ms = _extract_bigquery_info_from_metadata(
                dagster_event.metadata
            )
            if bytes_billed or slots_ms:
                asset_info_by_job_id[job_id] = (asset_key, partition)
                cost_info = BigQueryCostInfo(asset_key, partition, job_id, slots_ms, bytes_billed)
                cost_by_asset[cost_info.asset_partition_key].append(cost_info)

        yield dagster_event

    if explicitly_query_information_schema and bigquery_client:
        marker_asset_key = marker_asset_key_for_job(context.job_def)
        run_results_json = dbt_cli_invocation.get_artifact("run_results.json")
        invocation_id = run_results_json["metadata"]["invocation_id"]
        assert bigquery_client
        try:
            cost_query = f"""
                SELECT job_id, SUM(total_bytes_billed) AS bytes_billed, SUM(total_slot_ms) AS slots_ms
                FROM `{bigquery_location}`.INFORMATION_SCHEMA.JOBS
                WHERE query like '%{invocation_id}%'
                GROUP BY job_id
            """
            context.log.info(f"Querying INFORMATION_SCHEMA.JOBS for bytes billed: {cost_query}")
            query_result = bigquery_client.query(cost_query)

            # overwrite cost_by_asset that is computed from the metadata
            cost_by_asset = defaultdict(list)
            for row in query_result:
                asset_key, partition = asset_info_by_job_id.get(
                    row.job_id, (marker_asset_key, None)
                )
                if row.bytes_billed or row.slots_ms:
                    cost_info = BigQueryCostInfo(
                        asset_key, partition, row.job_id, row.bytes_billed, row.slots_ms
                    )
                    cost_by_asset[cost_info.asset_partition_key].append(cost_info)
        except:
            context.log.exception("Could not query information_schema.jobs for bytes billed")

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
