from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pprint import pprint
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
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
)

from dagster_cloud.instance import DagsterCloudAgentInstance

from .dagster_snowflake_insights import (
    AssetMaterializationId,
    get_cost_data_for_hour,
    query_graphql_from_instance,
)

if TYPE_CHECKING:
    from dagster_snowflake import SnowflakeConnection

SNOWFLAKE_QUERY_HISTORY_LATENCY_SLA_MINS = 60 + 45


@dataclass
class SnowflakeInsightsDefinitions:
    assets: Sequence[AssetsDefinition]
    schedule: ScheduleDefinition


def create_snowflake_insights_asset_and_schedule(
    start_date: date,
    code_location_name: str,
    repository_name: str = "__repository__",
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    job_name: str = "snowflake_insights_import",
    dry_run=True,
    allow_partial_partitions=False,
    snowflake_resource_key: str = "snowflake",
    snowflake_usage_latency: int = SNOWFLAKE_QUERY_HISTORY_LATENCY_SLA_MINS,
) -> SnowflakeInsightsDefinitions:
    """Generates a pre-defined Dagster asset and schedule that can be used to import Snowflake cost
    data into Dagster Insights.

    The schedule will run hourly, and will query the Snowflake query_history table for all queries
    that ran in the hour starting at the scheduled time. It will then submit the cost data to
    Dagster Insights.

    Args:
        start_date (date): The date to start the partitioned schedule on. This should be the date
            that you began to track cost data alongside your dbt runs.
        code_location_name (str): The name of the code location that this asset should be associated
            with in Dagster Insights.
        repository_name (str): The name of the repository that this asset should be associated with
            in Dagster Insights.
        name (Optional[str]): The name of the asset. Defaults to "snowflake_query_history".
        group_name (Optional[str]): The name of the asset group. Defaults to the default group.
        job_name (str): The name of the job that will be created to run the schedule. Defaults to
            "snowflake_insights_import".
        dry_run (bool): If true, the schedule will print the cost data to stdout instead of
            submitting it to Dagster Insights. Defaults to True.
        snowflake_resource_key (str): The name of the snowflake resource key to use. Defaults to
            "snowflake".
        snowflake_usage_latency (int): The number of minutes to wait after the end of the hour
            before querying the Snowflake query_history table. This is necessary as the Snowflake
            query_history table is not immediately available after the end of the hour. Its latency
            has an SLA of 45 minutes. The default value is 105 minutes, which provides an hour buffer.
    """

    @asset(
        name=name,
        group_name=group_name,
        partitions_def=HourlyPartitionsDefinition(start_date=start_date.strftime("%Y-%m-%d-%H:%M")),
        required_resource_keys={snowflake_resource_key},
    )
    def poll_snowflake_query_history_hour(
        context: AssetExecutionContext,
    ) -> None:
        snowflake: "SnowflakeConnection" = getattr(context.resources, snowflake_resource_key)

        start_hour = context.partition_time_window.start
        end_hour = context.partition_time_window.end

        if not allow_partial_partitions:
            now = datetime.now().astimezone(timezone.utc)
            earliest_call_time = end_hour + timedelta(minutes=snowflake_usage_latency)
            if now < earliest_call_time:
                raise RuntimeError(
                    "This function was called before the Snowflake query_history table may be"
                    f" available. For hour starting {start_hour.isoformat()} you can call it"
                    f" starting at {earliest_call_time.isoformat()} (it is currently"
                    f" {now.isoformat()})"
                )

        instance = context.instance
        if not isinstance(instance, DagsterCloudAgentInstance):
            raise RuntimeError("This asset only functions in a running Dagster Cloud instance")

        costs = (
            get_cost_data_for_hour(
                snowflake,
                lambda query_text, variables: query_graphql_from_instance(
                    instance, query_text, variables
                ),
                start_hour,
                end_hour,
            )
            or []
        )
        if dry_run:
            pprint(costs)
        else:
            metrics_to_submit = []
            mat_by_step: Mapping[Tuple[str, str], List[Tuple[AssetMaterializationId, Any]]] = (
                defaultdict(list)
            )
            for mat, cost in costs:
                mat_by_step[(mat.run_id, mat.step_key)].append((mat, cost))

            for run_id, step_key in mat_by_step.keys():
                materializations = mat_by_step[(run_id, step_key)]
                for i in range(0, len(materializations), 5):
                    metrics_to_submit.append(
                        {
                            "repositoryName": repository_name,
                            "codeLocationName": code_location_name,
                            "stepKey": step_key,
                            "runId": run_id,
                            "assetMetricDefinitions": [
                                {
                                    "assetKey": mat.asset_key.to_string(),
                                    "assetGroup": "default",
                                    "metricValues": [
                                        {
                                            "metricName": "snowflake_credits",
                                            "metricValue": float(cost),
                                        }
                                    ],
                                }
                                for mat, cost in materializations[i : i + 5]
                            ],
                        }
                    )
                metrics_to_submit.append(
                    {
                        "repositoryName": repository_name,
                        "codeLocationName": code_location_name,
                        "stepKey": step_key,
                        "runId": run_id,
                        "jobMetricDefinitions": [
                            {
                                "metricValues": [
                                    {
                                        "metricName": "snowflake_credits",
                                        "metricValue": sum(
                                            [float(cost) for mat, cost in materializations]
                                        ),
                                    }
                                ],
                            }
                        ],
                    }
                )

            mutation = """
                mutation AddMetrics($metrics: [MetricInputs]) {
                    createOrUpdateMetrics(
                        metrics: $metrics
                    ) {
                        __typename
                        ... on CreateOrUpdateMetricsFailed {
                            message 
                        }
                    }
                }
            """

            result = query_graphql_from_instance(
                instance, query_text=mutation, variables={"metrics": metrics_to_submit}
            )

            assert (
                result["data"]["createOrUpdateMetrics"]["__typename"]
                == "CreateOrUpdateMetricsSuccess"
            ), result

    schedule = build_schedule_from_partitioned_job(
        job=define_asset_job(job_name, AssetSelection.assets(poll_snowflake_query_history_hour)),
        minute_of_hour=59,
    )
    # schedule may be a UnresolvedPartitionedAssetScheduleDefinition so we ignore the type check
    return SnowflakeInsightsDefinitions(assets=[poll_snowflake_query_history_hour], schedule=schedule)  # type: ignore
