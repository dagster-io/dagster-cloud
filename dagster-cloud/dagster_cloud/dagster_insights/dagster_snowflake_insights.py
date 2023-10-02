import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pprint import pprint
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from uuid import uuid4 as uuid

import requests
import yaml
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetObservation,
    AssetsDefinition,
    AssetSelection,
    ConfigurableResource,
    HourlyPartitionsDefinition,
    Output,
    ScheduleDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
)
from pydantic import Field

if TYPE_CHECKING:
    from dagster_dbt import DbtCliInvocation
    from dagster_snowflake import SnowflakeConnection

try:
    pass
except ImportError:
    pass


@dataclass(frozen=True, eq=True)
class AssetMaterializationId:
    run_id: str
    asset_key: AssetKey
    partition: Optional[str]
    step_key: str


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
    context: AssetExecutionContext,
    dbt_cli_invocation: "DbtCliInvocation",
    dagster_events: Optional[Iterable[Union[Output, AssetObservation, AssetCheckResult]]] = None,
    skip_config_check=False,
) -> Iterator[Union[Output, AssetObservation, AssetCheckResult]]:
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

    asset_and_partition_key_to_unique_id = {}
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
            asset_and_partition_key_to_unique_id[(asset_key, partition_key)] = unique_id
        elif isinstance(dagster_event, AssetObservation):
            unique_id = dagster_event.metadata["unique_id"].value
            asset_key = dagster_event.asset_key
            partition_key = dagster_event.partition
            asset_and_partition_key_to_unique_id[(asset_key, partition_key)] = unique_id

        yield dagster_event

    run_results_json = dbt_cli_invocation.get_artifact("run_results.json")
    invocation_id = run_results_json["metadata"]["invocation_id"]

    for (asset_key, partition), unique_id in asset_and_partition_key_to_unique_id.items():
        # must match the query-comment in dbt_project.yml
        opaque_id = f"{unique_id}:{invocation_id}"
        context.log_event(
            AssetObservation(
                asset_key=asset_key,
                partition=partition,
                metadata={f"{OPAQUE_ID_METADATA_KEY_PREFIX}{opaque_id}": True},
            )
        )


class GraphqlClientResource(ConfigurableResource):
    url: str = Field(
        description=(
            "The URL of the Dagster Cloud GraphQL endpoint for your organization. For example,"
            " https://hooli.dagster.cloud/prod/graphql"
        )
    )
    cloud_token: Optional[str] = Field(
        description="A Dagster Cloud agent token to submit metrics to the Dagster Cloud API."
    )

    def execute(self, query_text: str, variables=None) -> Dict[str, Any]:
        headers = {}
        if self.cloud_token is not None:
            headers["Dagster-Cloud-API-Token"] = self.cloud_token
        return requests.post(
            self.url,
            json={"operationName": None, "variables": variables or dict(), "query": query_text},
            headers=headers,
        ).json()


def get_opaque_ids_to_assets(
    graphql_client: GraphqlClientResource, min_datetime: datetime, max_datetime: datetime
) -> Iterable[Tuple[AssetMaterializationId, str]]:
    query = """
{
	assetsOrError {
    ... on AssetConnection {
      nodes {
        key {
            path
        }
        assetObservations(afterTimestampMillis:"%d", beforeTimestampMillis:"%d") {
          runId
          partition
          stepKey
          metadataEntries {
            ... on BoolMetadataEntry {
              label
            }
          }
        }
      }
    }
  }
}
    """ % (
        min_datetime.timestamp() * 1000,
        max_datetime.timestamp() * 1000,
    )

    response = graphql_client.execute(
        #        "http://localhost:3000/graphql",
        query,
        variables={},
    )

    for node in response["data"]["assetsOrError"]["nodes"]:
        asset_key = AssetKey.from_graphql_input(node["key"])
        if asset_key is None:
            # should never happen, but we must appease the type checker
            raise RuntimeError("asset_key is None, which should never happen")
        for observation in node["assetObservations"]:
            run_id = observation["runId"]
            partition = observation["partition"]
            step_key = observation["stepKey"]
            opaque_id = None
            for metadata in observation["metadataEntries"]:
                if metadata.get("label", "").startswith(OPAQUE_ID_METADATA_KEY_PREFIX):
                    opaque_id = metadata["label"][len(OPAQUE_ID_METADATA_KEY_PREFIX) :]
                    break
            if opaque_id is not None:
                yield (
                    AssetMaterializationId(
                        run_id=run_id, asset_key=asset_key, partition=partition, step_key=step_key
                    ),
                    opaque_id,
                )


SNOWFLAKE_QUERY_HISTORY_LATENCY_SLA = timedelta(minutes=45)
QUERY_HISTORY_TIME_PADDING = timedelta(hours=1)  # deal with desynchronized clocks


@dataclass
class SnowflakeInsightsDefinitions:
    assets: Sequence[AssetsDefinition]
    schedule: ScheduleDefinition


def get_cost_data_for_hour(
    context: AssetExecutionContext,
    snowflake: "SnowflakeConnection",
    graphql_client: GraphqlClientResource,
    start_hour: datetime,
    end_hour: datetime,
) -> List[Tuple[AssetMaterializationId, Any]]:
    queries = list(get_opaque_ids_to_assets(graphql_client, start_hour, end_hour))
    print(
        f"Found {len(queries)} queries to check from {start_hour.isoformat()} to"
        f" {end_hour.isoformat()}"
    )
    for am, opaque_id in queries:
        print(opaque_id, am.run_id)
    if len(queries) == 0:
        return []

    opaque_id_to_asset_materialization_id: Dict[str, AssetMaterializationId] = {}
    for asset_materialization_id, opaque_id in queries:
        opaque_id_to_asset_materialization_id[opaque_id] = asset_materialization_id

    opaque_ids_sql = rf"""
    regexp_substr_all(query_text, '{OPAQUE_ID_SQL_SIGIL}\\[\\[\\[(.*?)\\]\\]\\]', 1, 1, 'ce', 1)
    """.strip()

    sql = f"""
WITH
warehouse_sizes AS (
    SELECT 'X-Small' AS warehouse_size, 1 AS credits_per_hour UNION ALL
    SELECT 'Small' AS warehouse_size, 2 AS credits_per_hour UNION ALL
    SELECT 'Medium'  AS warehouse_size, 4 AS credits_per_hour UNION ALL
    SELECT 'Large' AS warehouse_size, 8 AS credits_per_hour UNION ALL
    SELECT 'X-Large' AS warehouse_size, 16 AS credits_per_hour UNION ALL
    SELECT '2X-Large' AS warehouse_size, 32 AS credits_per_hour UNION ALL
    SELECT '3X-Large' AS warehouse_size, 64 AS credits_per_hour UNION ALL
    SELECT '4X-Large' AS warehouse_size, 128 AS credits_per_hour UNION ALL
    SELECT 'missing' as warehouse_size, 0 as credits_per_hour
)
SELECT
    {opaque_ids_sql} as opaque_ids,
    qh.execution_time/(1000*60*60)*wh.credits_per_hour AS query_cost,
    query_id
FROM snowflake.account_usage.query_history AS qh
INNER JOIN warehouse_sizes AS wh
    ON ifnull(qh.warehouse_size, 'missing')=wh.warehouse_size
WHERE
    start_time >= '{start_hour - QUERY_HISTORY_TIME_PADDING}'
    AND start_time <= '{end_hour + QUERY_HISTORY_TIME_PADDING}'
HAVING ARRAY_SIZE(opaque_ids) > 0
        """

    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            result = cursor.execute(sql)
            assert result
            results = result.fetchall()

    costs: List[Tuple[AssetMaterializationId, Any]] = []

    print(
        f"{len(results) if results else 0} annotated queries returned from snowflake query_history"
    )

    if not results:
        return []

    total = 0
    for result_opaque_ids, result_cost, query_id in results:
        opaque_ids = json.loads(result_opaque_ids)
        # TODO: is this cost splitting logic correct?
        cost = result_cost / len(opaque_ids)
        total += len(opaque_ids)
        for opaque_id in opaque_ids:
            print(opaque_id, query_id)
            if opaque_id in opaque_id_to_asset_materialization_id:
                costs.append((opaque_id_to_asset_materialization_id[opaque_id], cost))

    print(
        f"Reported costs for {len(costs)} of {total} asset materializations found in the"
        " query_history."
    )
    return costs


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
    graphql_client_resource_key: str = "graphql_client",
) -> SnowflakeInsightsDefinitions:
    @asset(
        name=name,
        group_name=group_name,
        partitions_def=HourlyPartitionsDefinition(start_date=start_date.strftime("%Y-%m-%d-%H:%M")),
        required_resource_keys={snowflake_resource_key, graphql_client_resource_key},
    )
    def poll_snowflake_query_history_hour(
        context: AssetExecutionContext,
    ) -> None:
        snowflake: "SnowflakeConnection" = getattr(context.resources, snowflake_resource_key)
        graphql_client: GraphqlClientResource = getattr(
            context.resources, graphql_client_resource_key
        )

        start_hour = context.partition_time_window.start
        end_hour = context.partition_time_window.end

        if not allow_partial_partitions:
            now = datetime.now().astimezone(timezone.utc)
            earliest_call_time = end_hour + SNOWFLAKE_QUERY_HISTORY_LATENCY_SLA
            if now < earliest_call_time:
                raise RuntimeError(
                    "This function was called before the Snowflake query_history table may be"
                    f" available. For hour starting {start_hour.isoformat()} you can call it"
                    f" starting at {earliest_call_time.isoformat()} (it is currently"
                    f" {now.isoformat()})"
                )

        costs = (
            get_cost_data_for_hour(context, snowflake, graphql_client, start_hour, end_hour) or []
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

            result = graphql_client.execute(
                query_text=mutation, variables={"metrics": metrics_to_submit}
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
