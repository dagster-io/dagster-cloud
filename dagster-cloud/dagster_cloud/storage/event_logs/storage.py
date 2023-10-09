import json
from collections import defaultdict
from os import getenv
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)
from uuid import uuid4

import dagster._check as check
from dagster import AssetCheckKey, DagsterInvalidInvocationError
from dagster._core.assets import AssetDetails
from dagster._core.definitions.events import AssetKey, ExpectationResult
from dagster._core.event_api import EventLogRecord, EventRecordsFilter, RunShardedEventsCursor
from dagster._core.events import DagsterEvent, DagsterEventType
from dagster._core.events.log import EventLogEntry
from dagster._core.execution.stats import RunStepKeyStatsSnapshot, RunStepMarker, StepEventStatus
from dagster._core.storage.asset_check_execution_record import AssetCheckExecutionRecord
from dagster._core.storage.dagster_run import DagsterRunStatsSnapshot
from dagster._core.storage.event_log.base import (
    AssetEntry,
    AssetRecord,
    EventLogConnection,
    EventLogStorage,
)
from dagster._core.storage.partition_status_cache import AssetStatusCacheValue
from dagster._serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    serialize_value,
)
from dagster._serdes.serdes import deserialize_value
from dagster._utils import datetime_as_float, utc_datetime_from_timestamp
from dagster._utils.concurrency import (
    ConcurrencyClaimStatus,
    ConcurrencyKeyInfo,
    ConcurrencySlotStatus,
)
from dagster._utils.error import SerializableErrorInfo
from dagster._utils.merger import merge_dicts
from dagster_cloud_cli.core.errors import GraphQLStorageError
from typing_extensions import Self

from dagster_cloud.storage.event_logs.utils import truncate_event

from .queries import (
    ADD_DYNAMIC_PARTITIONS_MUTATION,
    CHECK_CONCURRENCY_CLAIM_QUERY,
    CLAIM_CONCURRENCY_SLOT_MUTATION,
    DELETE_DYNAMIC_PARTITION_MUTATION,
    DELETE_EVENTS_MUTATION,
    ENABLE_SECONDARY_INDEX_MUTATION,
    FREE_CONCURRENCY_SLOT_FOR_STEP_MUTATION,
    FREE_CONCURRENCY_SLOTS_FOR_RUN_MUTATION,
    GET_ALL_ASSET_KEYS_QUERY,
    GET_ASSET_RECORDS_QUERY,
    GET_CONCURRENCY_INFO_QUERY,
    GET_CONCURRENCY_KEYS_QUERY,
    GET_DYNAMIC_PARTITIONS_QUERY,
    GET_EVENT_RECORDS_QUERY,
    GET_EVENT_TAGS_FOR_ASSET,
    GET_LATEST_ASSET_PARTITION_MATERIALIZATION_ATTEMPTS_WITHOUT_MATERIALIZATIONS,
    GET_LATEST_MATERIALIZATION_EVENTS_QUERY,
    GET_LATEST_STORAGE_ID_BY_PARTITION,
    GET_LATEST_TAGS_BY_PARTITION,
    GET_MATERIALIZATION_COUNT_BY_PARTITION,
    GET_MATERIALIZED_PARTITIONS,
    GET_RECORDS_FOR_RUN_QUERY,
    GET_STATS_FOR_RUN_QUERY,
    GET_STEP_STATS_FOR_RUN_QUERY,
    HAS_ASSET_KEY_QUERY,
    HAS_DYNAMIC_PARTITION_QUERY,
    IS_ASSET_AWARE_QUERY,
    IS_PERSISTENT_QUERY,
    REINDEX_MUTATION,
    SET_CONCURRENCY_SLOTS_MUTATION,
    STORE_EVENT_MUTATION,
    UPDATE_ASSET_CACHED_STATUS_DATA_MUTATION,
    UPGRADE_EVENT_LOG_STORAGE_MUTATION,
    WIPE_ASSET_CACHED_STATUS_DATA_MUTATION,
    WIPE_ASSET_MUTATION,
    WIPE_EVENT_LOG_STORAGE_MUTATION,
)


def _input_for_serializable_error_info(serializable_error_info: Optional[SerializableErrorInfo]):
    check.opt_inst_param(serializable_error_info, "serializable_error_info", SerializableErrorInfo)

    if serializable_error_info is None:
        return None

    return {
        "message": serializable_error_info.message,
        "stack": serializable_error_info.stack,
        "clsName": serializable_error_info.cls_name,
        "cause": _input_for_serializable_error_info(serializable_error_info.cause),
    }


def _input_for_dagster_event(dagster_event: Optional[DagsterEvent]):
    if dagster_event is None:
        return None

    return {
        "eventTypeValue": dagster_event.event_type_value,
        "pipelineName": dagster_event.job_name,
        "stepHandleKey": dagster_event.step_handle.to_key() if dagster_event.step_handle else None,
        "solidHandleId": (
            dagster_event.node_handle.to_string() if dagster_event.node_handle else None
        ),
        "stepKindValue": dagster_event.step_kind_value,
        "loggingTags": (
            json.dumps(dagster_event.logging_tags) if dagster_event.logging_tags else None
        ),
        "eventSpecificData": (
            serialize_value(dagster_event.event_specific_data)
            if dagster_event.event_specific_data
            else None
        ),
        "message": dagster_event.message,
        "pid": dagster_event.pid,
        "stepKey": dagster_event.step_key,
    }


def _event_log_entry_from_graphql(graphene_event_log_entry: Dict) -> EventLogEntry:
    check.dict_param(graphene_event_log_entry, "graphene_event_log_entry")

    return EventLogEntry(
        error_info=(
            deserialize_value(
                check.str_elem(graphene_event_log_entry, "errorInfo"), SerializableErrorInfo
            )
            if graphene_event_log_entry.get("errorInfo") is not None
            else None
        ),
        level=check.int_elem(graphene_event_log_entry, "level"),
        user_message=check.str_elem(graphene_event_log_entry, "userMessage"),
        run_id=check.str_elem(graphene_event_log_entry, "runId"),
        timestamp=check.float_elem(graphene_event_log_entry, "timestamp"),
        step_key=check.opt_str_elem(graphene_event_log_entry, "stepKey"),
        job_name=check.opt_str_elem(graphene_event_log_entry, "pipelineName"),
        dagster_event=(
            deserialize_value(
                check.str_elem(graphene_event_log_entry, "dagsterEvent"),
                DagsterEvent,
            )
            if graphene_event_log_entry.get("dagsterEvent") is not None
            else None
        ),
    )


def _get_event_records_filter_input(
    event_records_filter,
) -> Optional[Dict[str, Any]]:
    check.opt_inst_param(event_records_filter, "event_records_filter", EventRecordsFilter)

    if event_records_filter is None:
        return None

    run_updated_timestamp = None
    if isinstance(event_records_filter.after_cursor, RunShardedEventsCursor):
        # we should parse this correctly, even if this is a vestigial field that only has semantic
        # meaning in the context of a run-sharded cursor against a SQLite backed event-log
        updated_dt = event_records_filter.after_cursor.run_updated_after
        run_updated_timestamp = (
            updated_dt.timestamp() if updated_dt.tzinfo else datetime_as_float(updated_dt)
        )

    return {
        "eventType": (
            event_records_filter.event_type.value if event_records_filter.event_type else None
        ),
        "assetKey": (
            event_records_filter.asset_key.to_string() if event_records_filter.asset_key else None
        ),
        "assetPartitions": event_records_filter.asset_partitions,
        "afterTimestamp": event_records_filter.after_timestamp,
        "beforeTimestamp": event_records_filter.before_timestamp,
        "beforeCursor": (
            event_records_filter.before_cursor.id
            if isinstance(event_records_filter.before_cursor, RunShardedEventsCursor)
            else event_records_filter.before_cursor
        ),
        "afterCursor": (
            event_records_filter.after_cursor.id
            if isinstance(event_records_filter.after_cursor, RunShardedEventsCursor)
            else event_records_filter.after_cursor
        ),
        "runUpdatedAfter": run_updated_timestamp,
        "tags": (
            [
                merge_dicts(
                    {"key": tag_key},
                    ({"value": tag_value} if isinstance(tag_value, str) else {"values": tag_value}),
                )
                for tag_key, tag_value in event_records_filter.tags.items()
            ]
            if event_records_filter.tags
            else None
        ),
        "storageIds": event_records_filter.storage_ids,
    }


def _event_record_from_graphql(graphene_event_record: Dict) -> EventLogRecord:
    check.dict_param(graphene_event_record, "graphene_event_record")

    return EventLogRecord(
        storage_id=check.int_elem(graphene_event_record, "storageId"),
        event_log_entry=_event_log_entry_from_graphql(graphene_event_record["eventLogEntry"]),
    )


def _asset_entry_from_graphql(graphene_asset_entry: Dict) -> AssetEntry:
    check.dict_param(graphene_asset_entry, "graphene_asset_entry")
    return AssetEntry(
        asset_key=AssetKey(graphene_asset_entry["assetKey"]["path"]),
        last_materialization_record=(
            _event_record_from_graphql(graphene_asset_entry["lastMaterializationRecord"])
            if graphene_asset_entry["lastMaterializationRecord"]
            else None
        ),
        last_run_id=graphene_asset_entry["lastRunId"],
        asset_details=(
            AssetDetails(
                last_wipe_timestamp=graphene_asset_entry["assetDetails"]["lastWipeTimestamp"]
            )
            if graphene_asset_entry["assetDetails"]
            else None
        ),
        cached_status=(
            AssetStatusCacheValue(
                latest_storage_id=graphene_asset_entry["cachedStatus"]["latestStorageId"],
                partitions_def_id=graphene_asset_entry["cachedStatus"]["partitionsDefId"],
                serialized_materialized_partition_subset=graphene_asset_entry["cachedStatus"][
                    "serializedMaterializedPartitionSubset"
                ],
                serialized_failed_partition_subset=graphene_asset_entry["cachedStatus"][
                    "serializedFailedPartitionSubset"
                ],
                serialized_in_progress_partition_subset=graphene_asset_entry["cachedStatus"][
                    "serializedInProgressPartitionSubset"
                ],
                earliest_in_progress_materialization_event_id=graphene_asset_entry["cachedStatus"][
                    "earliestInProgressMaterializationEventId"
                ],
            )
            if graphene_asset_entry["cachedStatus"]
            else None
        ),
    )


def _asset_record_from_graphql(graphene_asset_record: Dict) -> AssetRecord:
    check.dict_param(graphene_asset_record, "graphene_asset_record")
    return AssetRecord(
        storage_id=graphene_asset_record["storageId"],
        asset_entry=_asset_entry_from_graphql(graphene_asset_record["assetEntry"]),
    )


class GraphQLEventLogStorage(EventLogStorage, ConfigurableClass):
    def __init__(self, inst_data=None, override_graphql_client=None):
        """Initialize this class directly only for test. Use the ConfigurableClass machinery to
        init from instance yaml.
        """
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self._override_graphql_client = override_graphql_client

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @classmethod
    def from_config_value(cls, inst_data: ConfigurableClassData, config_value: Any) -> Self:
        return GraphQLEventLogStorage(inst_data=inst_data)

    @property
    def _graphql_client(self):
        return (
            self._override_graphql_client
            if self._override_graphql_client
            else self._instance.graphql_client
        )

    def _execute_query(self, query, variables=None, headers=None):
        res = self._graphql_client.execute(
            query,
            variable_values=variables,
            headers=headers,
        )
        if "errors" in res:
            raise GraphQLStorageError(res)
        return res

    def get_records_for_run(
        self,
        run_id: str,
        cursor: Optional[str] = None,
        of_type: Optional[Union[DagsterEventType, Set[DagsterEventType]]] = None,
        limit: Optional[int] = None,
        ascending: bool = True,
    ) -> EventLogConnection:
        check.invariant(not of_type or isinstance(of_type, (DagsterEventType, frozenset, set)))

        is_of_type_set = isinstance(of_type, (set, frozenset))

        res = self._execute_query(
            GET_RECORDS_FOR_RUN_QUERY,
            variables={
                "runId": check.str_param(run_id, "run_id"),
                "cursor": check.opt_str_param(cursor, "cursor"),
                "ofType": (
                    cast(DagsterEventType, of_type).value
                    if of_type and not is_of_type_set
                    else None
                ),
                "ofTypes": (
                    [dagster_type.value for dagster_type in cast(set, of_type)]
                    if of_type and is_of_type_set
                    else None
                ),
                "limit": limit,
                # only send False for back-compat since True is the default
                **({"ascending": False} if not ascending else {}),
            },
        )
        connection_data = res["data"]["eventLogs"]["getRecordsForRun"]
        return EventLogConnection(
            records=[_event_record_from_graphql(record) for record in connection_data["records"]],
            cursor=connection_data["cursor"],
            has_more=connection_data["hasMore"],
        )

    def get_stats_for_run(self, run_id: str) -> DagsterRunStatsSnapshot:
        res = self._execute_query(
            GET_STATS_FOR_RUN_QUERY, variables={"runId": check.str_param(run_id, "run_id")}
        )
        stats = res["data"]["eventLogs"]["getStatsForRun"]
        return DagsterRunStatsSnapshot(
            run_id=check.str_elem(stats, "runId"),
            steps_succeeded=check.int_elem(stats, "stepsSucceeded"),
            steps_failed=check.int_elem(stats, "stepsFailed"),
            materializations=check.int_elem(stats, "materializations"),
            expectations=check.int_elem(stats, "expectations"),
            enqueued_time=check.opt_float_elem(stats, "enqueuedTime"),
            launch_time=check.opt_float_elem(stats, "launchTime"),
            start_time=check.opt_float_elem(stats, "startTime"),
            end_time=check.opt_float_elem(stats, "endTime"),
        )

    def get_step_stats_for_run(
        self, run_id: str, step_keys: Optional[List[str]] = None
    ) -> List[RunStepKeyStatsSnapshot]:
        res = self._execute_query(
            GET_STEP_STATS_FOR_RUN_QUERY,
            variables={
                "runId": check.str_param(run_id, "run_id"),
                "stepKeys": check.opt_list_param(step_keys, "step_keys", of_type=str),
            },
        )
        step_stats = res["data"]["eventLogs"]["getStepStatsForRun"]
        return [
            RunStepKeyStatsSnapshot(
                run_id=check.str_elem(stats, "runId"),
                step_key=check.str_elem(stats, "stepKey"),
                status=(
                    getattr(StepEventStatus, check.str_elem(stats, "status"))
                    if stats.get("status") is not None
                    else None
                ),
                start_time=check.opt_float_elem(stats, "startTime"),
                end_time=check.opt_float_elem(stats, "endTime"),
                materialization_events=[
                    deserialize_value(materialization_event, EventLogEntry)
                    for materialization_event in check.opt_list_elem(
                        stats, "materializationEvents", of_type=str
                    )
                ],
                expectation_results=[
                    deserialize_value(expectation_result, ExpectationResult)
                    for expectation_result in check.opt_list_elem(
                        stats, "expectationResults", of_type=str
                    )
                ],
                attempts=check.opt_int_elem(stats, "attempts"),
                attempts_list=[
                    deserialize_value(marker, RunStepMarker)
                    for marker in check.opt_list_elem(stats, "attemptsList", of_type=str)
                ],
                markers=[
                    deserialize_value(marker, RunStepMarker)
                    for marker in check.opt_list_elem(stats, "markers", of_type=str)
                ],
            )
            for stats in step_stats
        ]

    def store_event(self, event: EventLogEntry):
        check.inst_param(event, "event", EventLogEntry)

        event = truncate_event(event)

        headers = {}
        # env var opt-in for experimental feature
        if getenv("DAGSTER_CLOUD_IDEMPOTENT_STORE_EVENT"):
            headers = {"Idempotency-Key": str(uuid4())}

        self._execute_query(
            STORE_EVENT_MUTATION,
            variables={
                "eventRecord": {
                    "errorInfo": _input_for_serializable_error_info(event.error_info),
                    "level": event.level,
                    "userMessage": event.user_message,
                    "runId": event.run_id,
                    "timestamp": event.timestamp,
                    "stepKey": event.step_key,
                    "pipelineName": event.job_name,
                    "dagsterEvent": _input_for_dagster_event(event.dagster_event),
                }
            },
            headers=headers,
        )

    def delete_events(self, run_id: str):
        self._execute_query(
            DELETE_EVENTS_MUTATION, variables={"runId": check.str_param(run_id, "run_id")}
        )
        return

    def upgrade(self):
        return self._execute_query(UPGRADE_EVENT_LOG_STORAGE_MUTATION)

    def reindex_assets(self, print_fn: Optional[Callable] = lambda _: None, force: bool = False):
        return self._execute_query(
            REINDEX_MUTATION, variables={"force": check.bool_param(force, "force")}
        )

    def reindex_events(self, print_fn: Optional[Callable] = lambda _: None, force: bool = False):
        return self._execute_query(
            REINDEX_MUTATION, variables={"force": check.bool_param(force, "force")}
        )

    def wipe(self):
        return self._execute_query(WIPE_EVENT_LOG_STORAGE_MUTATION)

    def watch(self, run_id: str, cursor: str, callback: Callable):
        raise NotImplementedError("Not callable from user cloud")

    def end_watch(self, run_id: str, handler: Callable):
        raise NotImplementedError("Not callable from user cloud")

    def enable_secondary_index(self, name: str, run_id: Optional[str] = None):
        return self._execute_query(
            ENABLE_SECONDARY_INDEX_MUTATION,
            variables={
                "name": check.str_param(name, "name"),
                "runId": check.str_param(run_id, "run_id"),
            },
        )

    @property
    def is_persistent(self) -> bool:
        res = self._execute_query(IS_PERSISTENT_QUERY)
        return res["data"]["eventLogs"]["isPersistent"]

    @property
    def is_asset_aware(self) -> bool:
        res = self._execute_query(IS_ASSET_AWARE_QUERY)
        return res["data"]["eventLogs"]["isAssetAware"]

    def get_event_records(
        self,
        event_records_filter: Optional[EventRecordsFilter] = None,
        limit: Optional[int] = None,
        ascending: Optional[bool] = False,
    ) -> Iterable[EventLogRecord]:
        check.opt_inst_param(event_records_filter, "event_records_filter", EventRecordsFilter)
        check.opt_int_param(limit, "limit")
        check.bool_param(ascending, "ascending")

        res = self._execute_query(
            GET_EVENT_RECORDS_QUERY,
            variables={
                "eventRecordsFilter": _get_event_records_filter_input(event_records_filter),
                "limit": limit,
                "ascending": ascending,
            },
        )
        return [
            _event_record_from_graphql(result)
            for result in res["data"]["eventLogs"]["getEventRecords"]
        ]

    def get_asset_records(
        self, asset_keys: Optional[Sequence[AssetKey]] = None
    ) -> Iterable[AssetRecord]:
        res = self._execute_query(
            GET_ASSET_RECORDS_QUERY,
            variables={
                "assetKeys": (
                    [asset_key.to_string() for asset_key in asset_keys] if asset_keys else []
                )
            },
        )

        return [
            _asset_record_from_graphql(result)
            for result in res["data"]["eventLogs"]["getAssetRecords"]
        ]

    def has_asset_key(self, asset_key: AssetKey):
        check.inst_param(asset_key, "asset_key", AssetKey)

        res = self._execute_query(
            HAS_ASSET_KEY_QUERY, variables={"assetKey": asset_key.to_string()}
        )
        return res["data"]["eventLogs"]["hasAssetKey"]

    def all_asset_keys(self) -> Iterable[AssetKey]:
        res = self._execute_query(GET_ALL_ASSET_KEYS_QUERY)
        return [
            check.not_none(AssetKey.from_db_string(asset_key_string))
            for asset_key_string in res["data"]["eventLogs"]["getAllAssetKeys"]
        ]

    def get_latest_materialization_events(
        self, asset_keys: Sequence[AssetKey]
    ) -> Mapping[AssetKey, Optional[EventLogEntry]]:
        check.list_param(asset_keys, "asset_keys", of_type=AssetKey)

        res = self._execute_query(
            GET_LATEST_MATERIALIZATION_EVENTS_QUERY,
            variables={"assetKeys": [asset_key.to_string() for asset_key in asset_keys]},
        )

        result = {}
        for entry in res["data"]["eventLogs"]["getLatestMaterializationEvents"]:
            event = _event_log_entry_from_graphql(entry)
            if event.dagster_event and event.dagster_event.asset_key is not None:
                result[event.dagster_event.asset_key] = event

        return result

    def can_cache_asset_status_data(self) -> bool:
        # cached_status_data column exists in the asset_key table
        return True

    def update_asset_cached_status_data(
        self, asset_key: AssetKey, cache_values: AssetStatusCacheValue
    ) -> None:
        self._execute_query(
            UPDATE_ASSET_CACHED_STATUS_DATA_MUTATION,
            variables={
                "assetKey": asset_key.to_string(),
                "cacheValues": {
                    "latestStorageId": cache_values.latest_storage_id,
                    "partitionsDefId": cache_values.partitions_def_id,
                    "serializedMaterializedPartitionSubset": (
                        cache_values.serialized_materialized_partition_subset
                    ),
                    "serializedFailedPartitionSubset": (
                        cache_values.serialized_failed_partition_subset
                    ),
                    "serializedInProgressPartitionSubset": (
                        cache_values.serialized_in_progress_partition_subset
                    ),
                    "earliestInProgressMaterializationEventId": (
                        cache_values.earliest_in_progress_materialization_event_id
                    ),
                },
            },
        )

    def wipe_asset_cached_status(self, asset_key: AssetKey) -> None:
        res = self._execute_query(
            WIPE_ASSET_CACHED_STATUS_DATA_MUTATION, variables={"assetKey": asset_key.to_string()}
        )
        return res

    def get_materialized_partitions(
        self,
        asset_key: AssetKey,
        before_cursor: Optional[int] = None,
        after_cursor: Optional[int] = None,
    ) -> Set[str]:
        check.inst_param(asset_key, "asset_key", AssetKey)
        check.opt_int_param(before_cursor, "before_cursor")
        check.opt_int_param(after_cursor, "after_cursor")
        res = self._execute_query(
            GET_MATERIALIZED_PARTITIONS,
            variables={
                "assetKey": asset_key.to_string(),
                "beforeCursor": before_cursor,
                "afterCursor": after_cursor,
            },
        )
        materialized_partitions = res["data"]["eventLogs"]["getMaterializedPartitions"]
        return set(materialized_partitions)

    def get_materialization_count_by_partition(
        self,
        asset_keys: Sequence[AssetKey],
        after_cursor: Optional[int] = None,
    ) -> Mapping[AssetKey, Mapping[str, int]]:
        check.list_param(asset_keys, "asset_keys", of_type=AssetKey)
        check.opt_int_param(after_cursor, "after_cursor")

        res = self._execute_query(
            GET_MATERIALIZATION_COUNT_BY_PARTITION,
            variables={
                "assetKeys": [asset_key.to_string() for asset_key in asset_keys],
                "afterCursor": after_cursor,
            },
        )

        materialization_count_result = res["data"]["eventLogs"][
            "getMaterializationCountByPartition"
        ]

        materialization_count_by_partition: Dict[AssetKey, Dict[str, int]] = {
            asset_key: {} for asset_key in asset_keys
        }
        for asset_count in materialization_count_result:
            asset_key = check.not_none(AssetKey.from_db_string(asset_count["assetKey"]))
            for graphene_partition_count in asset_count["materializationCountByPartition"]:
                materialization_count_by_partition[asset_key][
                    graphene_partition_count["partition"]
                ] = graphene_partition_count["materializationCount"]

        return materialization_count_by_partition

    def get_latest_storage_id_by_partition(
        self, asset_key: AssetKey, event_type: DagsterEventType
    ) -> Mapping[str, int]:
        res = self._execute_query(
            GET_LATEST_STORAGE_ID_BY_PARTITION,
            variables={
                "assetKey": asset_key.to_string(),
                "eventType": event_type.value,
            },
        )
        latest_storage_id_result = res["data"]["eventLogs"]["getLatestStorageIdByPartition"]
        latest_storage_id_by_partition: Dict[str, int] = {}

        for graphene_latest_storage_id in latest_storage_id_result:
            latest_storage_id_by_partition[graphene_latest_storage_id["partition"]] = (
                graphene_latest_storage_id["storageId"]
            )

        return latest_storage_id_by_partition

    def get_latest_tags_by_partition(
        self,
        asset_key: AssetKey,
        event_type: DagsterEventType,
        tag_keys: Optional[Sequence[str]] = None,
        asset_partitions: Optional[Sequence[str]] = None,
        before_cursor: Optional[int] = None,
        after_cursor: Optional[int] = None,
    ) -> Mapping[str, Mapping[str, str]]:
        res = self._execute_query(
            GET_LATEST_TAGS_BY_PARTITION,
            variables={
                "assetKey": asset_key.to_string(),
                "eventType": event_type.value,
                "tagKeys": tag_keys,
                "assetPartitions": asset_partitions,
                "beforeCursor": before_cursor,
                "afterCursor": after_cursor,
            },
        )
        latest_tags_by_partition_result = res["data"]["eventLogs"]["getLatestTagsByPartition"]
        latest_tags_by_partition: Dict[str, Dict[str, str]] = defaultdict(dict)
        for tag_by_partition in latest_tags_by_partition_result:
            latest_tags_by_partition[tag_by_partition["partition"]][tag_by_partition["key"]] = (
                tag_by_partition["value"]
            )

        # convert defaultdict to dict
        return dict(latest_tags_by_partition)

    def get_latest_asset_partition_materialization_attempts_without_materializations(
        self, asset_key: AssetKey
    ) -> Mapping[str, Tuple[str, int]]:
        res = self._execute_query(
            GET_LATEST_ASSET_PARTITION_MATERIALIZATION_ATTEMPTS_WITHOUT_MATERIALIZATIONS,
            variables={
                "assetKey": asset_key.to_string(),
            },
        )
        return res["data"]["eventLogs"][
            "getLatestAssetPartitionMaterializationAttemptsWithoutMaterializations"
        ]

    def get_event_tags_for_asset(
        self,
        asset_key: AssetKey,
        filter_tags: Optional[Mapping[str, str]] = None,
        filter_event_id: Optional[int] = None,
    ) -> Sequence[Mapping[str, str]]:
        check.inst_param(asset_key, "asset_key", AssetKey)
        filter_tags = check.opt_mapping_param(
            filter_tags, "filter_tags", key_type=str, value_type=str
        )

        res = self._execute_query(
            GET_EVENT_TAGS_FOR_ASSET,
            variables={
                "assetKey": asset_key.to_string(),
                "filterTags": [{"key": key, "value": value} for key, value in filter_tags.items()],
                "filterEventId": filter_event_id,
            },
        )
        tags_result = res["data"]["eventLogs"]["getAssetEventTags"]
        return [
            {event_tag["key"]: event_tag["value"] for event_tag in event_tags["tags"]}
            for event_tags in tags_result
        ]

    def get_dynamic_partitions(self, partitions_def_name: str) -> Sequence[str]:
        check.str_param(partitions_def_name, "partitions_def_name")
        res = self._execute_query(
            GET_DYNAMIC_PARTITIONS_QUERY,
            variables={"partitionsDefName": partitions_def_name},
        )
        return res["data"]["eventLogs"]["getDynamicPartitions"]

    def has_dynamic_partition(self, partitions_def_name: str, partition_key: str) -> bool:
        check.str_param(partitions_def_name, "partitions_def_name")
        check.str_param(partition_key, "partition_key")
        res = self._execute_query(
            HAS_DYNAMIC_PARTITION_QUERY,
            variables={
                "partitionsDefName": partitions_def_name,
                "partitionKey": partition_key,
            },
        )
        return res["data"]["eventLogs"]["hasDynamicPartition"]

    def add_dynamic_partitions(
        self, partitions_def_name: str, partition_keys: Sequence[str]
    ) -> None:
        check.str_param(partitions_def_name, "partitions_def_name")
        check.sequence_param(partition_keys, "partition_keys", of_type=str)
        self._execute_query(
            ADD_DYNAMIC_PARTITIONS_MUTATION,
            variables={
                "partitionsDefName": partitions_def_name,
                "partitionKeys": partition_keys,
            },
        )

    def delete_dynamic_partition(self, partitions_def_name: str, partition_key: str) -> None:
        check.str_param(partitions_def_name, "partitions_def_name")
        check.str_param(partition_key, "partition_key")
        self._execute_query(
            DELETE_DYNAMIC_PARTITION_MUTATION,
            variables={
                "partitionsDefName": partitions_def_name,
                "partitionKey": partition_key,
            },
        )

    def wipe_asset(self, asset_key: AssetKey):
        res = self._execute_query(
            WIPE_ASSET_MUTATION, variables={"assetKey": asset_key.to_string()}
        )
        return res

    @property
    def supports_global_concurrency_limits(self) -> bool:
        return True

    def set_concurrency_slots(self, concurrency_key: str, num: int) -> None:
        check.str_param(concurrency_key, "concurrency_key")
        check.int_param(num, "num")
        res = self._execute_query(
            SET_CONCURRENCY_SLOTS_MUTATION,
            variables={"concurrencyKey": concurrency_key, "num": num},
        )
        result = res["data"]["eventLogs"]["SetConcurrencySlots"]
        error = result.get("error")

        if error:
            if error["className"] == "DagsterInvalidInvocationError":
                raise DagsterInvalidInvocationError(error["message"])
            else:
                raise GraphQLStorageError(res)
        return res

    def get_concurrency_keys(self) -> Set[str]:
        res = self._execute_query(GET_CONCURRENCY_KEYS_QUERY)
        return set(res["data"]["eventLogs"]["getConcurrencyKeys"])

    def get_concurrency_info(self, concurrency_key: str) -> ConcurrencyKeyInfo:
        check.str_param(concurrency_key, "concurrency_key")
        res = self._execute_query(
            GET_CONCURRENCY_INFO_QUERY,
            variables={"concurrencyKey": concurrency_key},
        )
        info = res["data"]["eventLogs"]["getConcurrencyInfo"]
        return ConcurrencyKeyInfo(
            concurrency_key=concurrency_key,
            slot_count=info["slotCount"],
            active_slot_count=info["activeSlotCount"],
            active_run_ids=set(info["activeRunIds"]),
            pending_step_count=info["pendingStepCount"],
            pending_run_ids=set(info["pendingStepRunIds"]),
            assigned_step_count=info["assignedStepCount"],
            assigned_run_ids=set(info["assignedStepRunIds"]),
        )

    def claim_concurrency_slot(
        self, concurrency_key: str, run_id: str, step_key: str, priority: Optional[int] = None
    ) -> ConcurrencyClaimStatus:
        check.str_param(concurrency_key, "concurrency_key")
        check.str_param(run_id, "run_id")
        check.str_param(step_key, "step_key")
        check.opt_int_param(priority, "priority")
        res = self._execute_query(
            CLAIM_CONCURRENCY_SLOT_MUTATION,
            variables={
                "concurrencyKey": concurrency_key,
                "runId": run_id,
                "stepKey": step_key,
                "priority": priority,
            },
        )
        claim_status = res["data"]["eventLogs"]["ClaimConcurrencySlot"]["status"]
        return ConcurrencyClaimStatus(
            concurrency_key=concurrency_key,
            slot_status=ConcurrencySlotStatus(claim_status["slotStatus"]),
            priority=claim_status["priority"],
            assigned_timestamp=(
                utc_datetime_from_timestamp(claim_status["assignedTimestamp"])
                if claim_status["assignedTimestamp"]
                else None
            ),
            enqueued_timestamp=(
                utc_datetime_from_timestamp(claim_status["enqueuedTimestamp"])
                if claim_status["enqueuedTimestamp"]
                else None
            ),
        )

    def check_concurrency_claim(
        self, concurrency_key: str, run_id: str, step_key: str
    ) -> ConcurrencyClaimStatus:
        check.str_param(concurrency_key, "concurrency_key")
        check.str_param(run_id, "run_id")
        check.str_param(step_key, "step_key")
        res = self._execute_query(
            CHECK_CONCURRENCY_CLAIM_QUERY,
            variables={
                "concurrencyKey": concurrency_key,
                "runId": run_id,
                "stepKey": step_key,
            },
        )
        claim_status = res["data"]["eventLogs"]["getCheckConcurrencyClaim"]
        return ConcurrencyClaimStatus(
            concurrency_key=concurrency_key,
            slot_status=ConcurrencySlotStatus(claim_status["slotStatus"]),
            priority=claim_status["priority"],
            assigned_timestamp=(
                utc_datetime_from_timestamp(claim_status["assignedTimestamp"])
                if claim_status["assignedTimestamp"]
                else None
            ),
            enqueued_timestamp=(
                utc_datetime_from_timestamp(claim_status["enqueuedTimestamp"])
                if claim_status["enqueuedTimestamp"]
                else None
            ),
        )

    def get_concurrency_run_ids(self) -> Set[str]:
        raise NotImplementedError("Not callable from user cloud")

    def free_concurrency_slots_for_run(self, run_id: str) -> None:
        check.str_param(run_id, "run_id")
        res = self._execute_query(
            FREE_CONCURRENCY_SLOTS_FOR_RUN_MUTATION, variables={"runId": run_id}
        )
        return res

    def free_concurrency_slot_for_step(self, run_id: str, step_key: str) -> None:
        check.str_param(run_id, "run_id")
        check.opt_str_param(step_key, "step_key")
        res = self._execute_query(
            FREE_CONCURRENCY_SLOT_FOR_STEP_MUTATION,
            variables={"runId": run_id, "stepKey": step_key},
        )
        return res

    def get_asset_check_execution_history(
        self,
        check_key: AssetCheckKey,
        limit: int,
        cursor: Optional[int] = None,
    ) -> Sequence[AssetCheckExecutionRecord]:
        raise NotImplementedError("Not callable from user cloud")

    def get_latest_asset_check_execution_by_key(
        self, asset_check_keys: Sequence[AssetCheckKey]
    ) -> Mapping[AssetCheckKey, AssetCheckExecutionRecord]:
        raise NotImplementedError("Not callable from user cloud")
