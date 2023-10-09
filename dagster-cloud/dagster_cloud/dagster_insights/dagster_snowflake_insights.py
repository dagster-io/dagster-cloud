import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
)

import requests
from dagster import (
    AssetKey,
    AssetsDefinition,
    DagsterInstance,
    ScheduleDefinition,
)

from dagster_cloud.agent.dagster_cloud_agent import DagsterCloudAgentInstance

from .dbt_wrapper import OPAQUE_ID_METADATA_KEY_PREFIX, OPAQUE_ID_SQL_SIGIL

if TYPE_CHECKING:
    from dagster_snowflake import SnowflakeConnection


@dataclass(frozen=True, eq=True)
class AssetMaterializationId:
    run_id: str
    asset_key: AssetKey
    partition: Optional[str]
    step_key: str


def get_url_and_token_from_instance(instance: DagsterInstance) -> Tuple[str, str]:
    if not isinstance(instance, DagsterCloudAgentInstance):
        raise RuntimeError("This asset only functions in a running Dagster Cloud instance")

    return f"{instance.dagit_url}graphql", instance.dagster_cloud_agent_token


def query_graphql_from_instance(
    instance: DagsterInstance, query_text: str, variables=None
) -> Dict[str, Any]:
    headers = {}

    url, cloud_token = get_url_and_token_from_instance(instance)

    headers["Dagster-Cloud-API-Token"] = cloud_token

    return requests.post(
        url,
        json={"operationName": None, "variables": variables or dict(), "query": query_text},
        headers=headers,
    ).json()


def get_opaque_ids_to_assets(
    query_gql: Callable[[str, Optional[Dict[str, Any]]], Dict[str, Any]],
    min_datetime: datetime,
    max_datetime: datetime,
) -> Iterable[Tuple[AssetMaterializationId, str]]:
    """Given access to the Dagster GraphQL API, queries for all asset materializations that occurred
    in the provided time range and returns a mapping from AssetMaterializationId to opaque
    Snowflake query ID.
    """
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
          timestamp
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

    response = query_gql(query, {})

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


QUERY_HISTORY_TIME_PADDING = timedelta(hours=1)  # deal with desynchronized clocks


@dataclass
class SnowflakeInsightsDefinitions:
    assets: Sequence[AssetsDefinition]
    schedule: ScheduleDefinition


def get_cost_data_for_hour(
    snowflake: "SnowflakeConnection",
    query_gql: Callable[[str, Optional[Dict[str, Any]]], Dict[str, Any]],
    start_hour: datetime,
    end_hour: datetime,
) -> List[Tuple[AssetMaterializationId, Any]]:
    """Given a date range, queries the Snowflake query_history table for all queries that were run
    during that time period and returns a mapping from AssetMaterializationId to the cost of the
    query that produced it, as estimated by Snowflake. The cost is in Snowflake credits.
    """
    queries = list(get_opaque_ids_to_assets(query_gql, start_hour, end_hour))
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
