from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pprint import pprint
from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
    Union,
)

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    AssetSelection,
    HourlyPartitionsDefinition,
    ScheduleDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
    fs_io_manager,
)

from .dagster_snowflake_insights import (
    get_cost_data_for_hour,
)
from .metrics_utils import put_cost_information

if TYPE_CHECKING:
    from dagster_snowflake import SnowflakeConnection

SNOWFLAKE_QUERY_HISTORY_LATENCY_SLA_MINS = 45


@dataclass
class SnowflakeInsightsDefinitions:
    assets: Sequence[AssetsDefinition]
    schedule: ScheduleDefinition


def create_snowflake_insights_asset_and_schedule(
    start_date: Union[datetime, date, str],
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    job_name: str = "snowflake_insights_import",
    dry_run=False,
    allow_partial_partitions=True,
    snowflake_resource_key: str = "snowflake",
    snowflake_usage_latency: int = SNOWFLAKE_QUERY_HISTORY_LATENCY_SLA_MINS,
    partition_end_offset_hrs: int = 1,
) -> SnowflakeInsightsDefinitions:
    """Generates a pre-defined Dagster asset and schedule that can be used to import Snowflake cost
    data into Dagster Insights.

    The schedule will run hourly, and will query the Snowflake query_history table for all queries
    that ran in the hour starting at the scheduled time. It will then submit the cost data to
    Dagster Insights.

    Args:
        start_date (Union[datetime, str]): The date to start the partitioned schedule on. This should be the date
            that you began to track cost data alongside your dbt runs.
        name (Optional[str]): The name of the asset. Defaults to "snowflake_query_history".
        group_name (Optional[str]): The name of the asset group. Defaults to the default group.
        job_name (str): The name of the job that will be created to run the schedule. Defaults to
            "snowflake_insights_import".
        dry_run (bool): If true, the schedule will print the cost data to stdout instead of
            submitting it to Dagster Insights. Defaults to True.
        snowflake_resource_key (str): The name of the snowflake resource key to use. Defaults to
            "snowflake".
        partition_end_offset_hrs (int): The number of additional hours to wait before querying
            Snowflake for the latest data. This is useful in case the Snowflake query_history table
            is not immediately available. Defaults to .
    """
    # for backcompat, this used to take `date`
    if isinstance(start_date, date):
        start_date = start_date.strftime("%Y-%m-%d-%H:%M")

    @asset(
        name=name,
        group_name=group_name,
        partitions_def=HourlyPartitionsDefinition(
            start_date=start_date, end_offset=-abs(partition_end_offset_hrs)
        ),
        required_resource_keys={snowflake_resource_key},
        io_manager_def=fs_io_manager,
    )
    def poll_snowflake_query_history_hour(
        context: AssetExecutionContext,
    ) -> None:
        snowflake: "SnowflakeConnection" = getattr(context.resources, snowflake_resource_key)

        start_hour = context.partition_time_window.start
        end_hour = context.partition_time_window.end

        now = datetime.now().astimezone(timezone.utc)
        earliest_call_time = end_hour + timedelta(minutes=snowflake_usage_latency)
        if now < earliest_call_time:
            err = (
                "Attempted to gather Snowflake usage information before the Snowflake query_history table may be"
                f" available. For hour starting {start_hour.isoformat()} you can call it"
                f" starting at {earliest_call_time.isoformat()} (it is currently"
                f" {now.isoformat()})"
            )
            if allow_partial_partitions:
                context.log.error(err)
            else:
                raise RuntimeError(err)

        costs = (
            get_cost_data_for_hour(
                snowflake,
                start_hour,
                end_hour,
            )
            or []
        )
        snowflake_query_end_time = datetime.now().astimezone(timezone.utc)
        context.log.info(
            f"Fetched query history information from {start_hour.isoformat()} to {end_hour.isoformat()} in {(snowflake_query_end_time - now).total_seconds()} seconds"
        )

        if dry_run:
            pprint(costs)
        else:
            context.log.info(
                f"Submitting cost information for {len(costs)} queries to Dagster Insights"
            )
            result = put_cost_information(
                context=context,
                metric_name="snowflake_credits",
                cost_information=costs,
                start=start_hour.timestamp(),
                end=end_hour.timestamp(),
            )
            if (
                result["submitCostInformationForMetrics"]["__typename"]
                != "CreateOrUpdateMetricsSuccess"
            ):
                raise RuntimeError("Failed to submit cost information", result)

    schedule = build_schedule_from_partitioned_job(
        job=define_asset_job(job_name, AssetSelection.assets(poll_snowflake_query_history_hour)),
        minute_of_hour=59,
    )
    # schedule may be a UnresolvedPartitionedAssetScheduleDefinition so we ignore the type check
    return SnowflakeInsightsDefinitions(
        assets=[poll_snowflake_query_history_hour],
        schedule=schedule,  # type: ignore
    )
