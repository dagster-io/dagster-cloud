import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
)

import dagster._check as check
from dagster import (
    AssetKey,
    AssetsDefinition,
    DagsterEventType,
    DagsterInstance,
    EventLogRecord,
    EventRecordsFilter,
    ScheduleDefinition,
)

from .snowflake_utils import OPAQUE_ID_METADATA_KEY_PREFIX, OPAQUE_ID_SQL_SIGIL

if TYPE_CHECKING:
    from dagster_snowflake import SnowflakeConnection

EVENT_RECORDS_LIMIT = 10000


@dataclass(frozen=True, eq=True)
class AssetMaterializationId:
    run_id: str
    asset_key: AssetKey
    partition: Optional[str]
    step_key: str


def get_opaque_ids_to_assets(
    instance: DagsterInstance,
    min_datetime: datetime,
    max_datetime: datetime,
) -> Iterable[Tuple[AssetMaterializationId, str]]:
    """Given access to the Dagster GraphQL API, queries for all asset materializations that occurred
    in the provided time range and returns a mapping from AssetMaterializationId to opaque
    Snowflake query ID.
    """
    records: Sequence[EventLogRecord] = []
    last_response = []
    first = True

    while len(last_response) == EVENT_RECORDS_LIMIT or first:
        last_response = instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION,
                before_timestamp=max_datetime.timestamp(),
                after_timestamp=min_datetime.timestamp(),
                after_cursor=last_response[-1].storage_id if last_response else None,
            ),
            limit=EVENT_RECORDS_LIMIT,
        )
        first = False
        records.extend(last_response)

    for record in records:
        asset_key = record.asset_key
        if asset_key is None:
            # should never happen, but we must appease the type checker
            raise RuntimeError("asset_key is None, which should never happen")
        run_id = check.not_none(record.run_id)
        partition = record.partition_key
        step_key = check.not_none(record.event_log_entry.step_key)
        opaque_id = None

        observation = check.not_none(record.event_log_entry.asset_observation)

        for metadata_key in observation.metadata:
            if metadata_key.startswith(OPAQUE_ID_METADATA_KEY_PREFIX):
                opaque_id = metadata_key[len(OPAQUE_ID_METADATA_KEY_PREFIX) :]
                break
        if opaque_id is not None:
            yield (
                AssetMaterializationId(
                    run_id=run_id,
                    asset_key=asset_key,
                    partition=partition,
                    step_key=step_key,
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
    start_hour: datetime,
    end_hour: datetime,
) -> List[Tuple[str, float, str]]:
    """Given a date range, queries the Snowflake query_history table for all queries that were run
    during that time period and returns a mapping from AssetMaterializationId to the cost of the
    query that produced it, as estimated by Snowflake. The cost is in Snowflake credits.
    """
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

    costs: List[Tuple[str, float, str]] = []

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
            costs.append((opaque_id, float(cost), query_id))

    print(
        f"Reported costs for {len(costs)} of {total} asset materializations found in the"
        " query_history."
    )
    return costs
