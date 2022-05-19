import json
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

import dagster._check as check
from dagster.core.assets import AssetDetails
from dagster.core.definitions.events import AssetKey, ExpectationResult
from dagster.core.events import DagsterEvent, DagsterEventType
from dagster.core.events.log import EventLogEntry
from dagster.core.execution.stats import RunStepKeyStatsSnapshot, RunStepMarker, StepEventStatus
from dagster.core.storage.event_log.base import (
    AssetEntry,
    AssetRecord,
    EventLogRecord,
    EventLogStorage,
    EventRecordsFilter,
    RunShardedEventsCursor,
    extract_asset_events_cursor,
)
from dagster.core.storage.pipeline_run import PipelineRunStatsSnapshot
from dagster.serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    deserialize_json_to_dagster_namedtuple,
    serialize_dagster_namedtuple,
)
from dagster.serdes.serdes import deserialize_as
from dagster.utils import datetime_as_float
from dagster.utils.error import SerializableErrorInfo
from dagster_cloud.storage.event_logs.utils import truncate_event

from ..errors import GraphQLStorageError
from .queries import (
    DELETE_EVENTS_MUTATION,
    ENABLE_SECONDARY_INDEX_MUTATION,
    END_WATCH_MUTATION,
    GET_ALL_ASSET_KEYS_QUERY,
    GET_ASSET_RECORDS_QUERY,
    GET_ASSET_RUN_IDS_QUERY,
    GET_EVENT_RECORDS_QUERY,
    GET_LATEST_MATERIALIZATION_EVENTS_QUERY,
    GET_LOGS_FOR_RUN_QUERY,
    GET_MATERIALIZATION_COUNT_BY_PARTITION,
    GET_STATS_FOR_RUN_QUERY,
    GET_STEP_STATS_FOR_RUN_QUERY,
    HAS_ASSET_KEY_QUERY,
    IS_ASSET_AWARE_QUERY,
    IS_PERSISTENT_QUERY,
    REINDEX_MUTATION,
    STORE_EVENT_MUTATION,
    UPGRADE_EVENT_LOG_STORAGE_MUTATION,
    WATCH_MUTATION,
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
        "pipelineName": dagster_event.pipeline_name,
        "stepHandleKey": dagster_event.step_handle.to_key() if dagster_event.step_handle else None,
        "solidHandleId": (
            dagster_event.solid_handle.to_string() if dagster_event.solid_handle else None
        ),
        "stepKindValue": dagster_event.step_kind_value,
        "loggingTags": (
            json.dumps(dagster_event.logging_tags) if dagster_event.logging_tags else None
        ),
        "eventSpecificData": (
            serialize_dagster_namedtuple(dagster_event.event_specific_data)
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
            deserialize_json_to_dagster_namedtuple(
                check.str_elem(graphene_event_log_entry, "errorInfo")
            )
            if graphene_event_log_entry.get("errorInfo") is not None
            else None
        ),
        level=check.int_elem(graphene_event_log_entry, "level"),
        user_message=check.str_elem(graphene_event_log_entry, "userMessage"),
        run_id=check.str_elem(graphene_event_log_entry, "runId"),
        timestamp=check.float_elem(graphene_event_log_entry, "timestamp"),
        step_key=check.opt_str_elem(graphene_event_log_entry, "stepKey"),
        pipeline_name=check.opt_str_elem(graphene_event_log_entry, "pipelineName"),
        dagster_event=(
            deserialize_json_to_dagster_namedtuple(
                check.str_elem(graphene_event_log_entry, "dagsterEvent")
            )
            if graphene_event_log_entry.get("dagsterEvent") is not None
            else None
        ),
    )


def _get_event_records_filter_input(event_records_filter) -> Optional[Dict[str, Any]]:
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
        "eventType": event_records_filter.event_type.value
        if event_records_filter.event_type
        else None,
        "assetKey": event_records_filter.asset_key.to_string()
        if event_records_filter.asset_key
        else None,
        "assetPartitions": event_records_filter.asset_partitions,
        "afterTimestamp": event_records_filter.after_timestamp,
        "beforeTimestamp": event_records_filter.before_timestamp,
        "beforeCursor": event_records_filter.before_cursor.id
        if isinstance(event_records_filter.before_cursor, RunShardedEventsCursor)
        else event_records_filter.before_cursor,
        "afterCursor": event_records_filter.after_cursor.id
        if isinstance(event_records_filter.after_cursor, RunShardedEventsCursor)
        else event_records_filter.after_cursor,
        "runUpdatedAfter": run_updated_timestamp,
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
        last_materialization=_event_log_entry_from_graphql(
            graphene_asset_entry["lastMaterialization"]
        )
        if graphene_asset_entry["lastMaterialization"]
        else None,
        last_run_id=graphene_asset_entry["lastRunId"],
        asset_details=AssetDetails(
            last_wipe_timestamp=graphene_asset_entry["assetDetails"]["lastWipeTimestamp"]
        )
        if graphene_asset_entry["assetDetails"]
        else None,
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
        init from instance yaml."""
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self._override_graphql_client = override_graphql_client

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data: ConfigurableClassData, config_value):
        return GraphQLEventLogStorage(inst_data=inst_data)

    @property
    def _graphql_client(self):
        return (
            self._override_graphql_client
            if self._override_graphql_client
            else self._instance.graphql_client
        )

    def _execute_query(self, query, variables=None):
        res = self._graphql_client.execute(query, variable_values=variables)
        if "errors" in res:
            raise GraphQLStorageError(res)
        return res

    def get_logs_for_run(
        self,
        run_id: str,
        cursor: Optional[int] = -1,
        of_type: Optional[Union[DagsterEventType, Set[DagsterEventType]]] = None,
        limit: Optional[int] = None,
    ) -> Iterable[EventLogEntry]:
        check.invariant(
            not of_type
            or isinstance(of_type, DagsterEventType)
            or isinstance(of_type, (frozenset, set))
        )

        is_of_type_set = isinstance(of_type, (set, frozenset))

        res = self._execute_query(
            GET_LOGS_FOR_RUN_QUERY,
            variables={
                "runId": check.str_param(run_id, "run_id"),
                "cursor": check.int_param(cursor, "cursor"),
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
            },
        )
        return [
            _event_log_entry_from_graphql(event)
            for event in res["data"]["eventLogs"]["getLogsForRun"]
        ]

    def get_stats_for_run(self, run_id: str) -> PipelineRunStatsSnapshot:
        res = self._execute_query(
            GET_STATS_FOR_RUN_QUERY, variables={"runId": check.str_param(run_id, "run_id")}
        )
        stats = res["data"]["eventLogs"]["getStatsForRun"]
        return PipelineRunStatsSnapshot(
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
                    deserialize_as(materialization_event, EventLogEntry)
                    for materialization_event in check.opt_list_elem(
                        stats, "materializationEvents", of_type=str
                    )
                ],
                expectation_results=[
                    deserialize_as(expectation_result, ExpectationResult)
                    for expectation_result in check.opt_list_elem(
                        stats, "expectationResults", of_type=str
                    )
                ],
                attempts=check.opt_int_elem(stats, "attempts"),
                attempts_list=[
                    deserialize_as(marker, RunStepMarker)
                    for marker in check.opt_list_elem(stats, "attemptsList", of_type=str)
                ],
                markers=[
                    deserialize_as(marker, RunStepMarker)
                    for marker in check.opt_list_elem(stats, "markers", of_type=str)
                ],
            )
            for stats in step_stats
        ]

    def store_event(self, event: EventLogEntry):
        check.inst_param(event, "event", EventLogEntry)

        event = truncate_event(event)

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
                    "pipelineName": event.pipeline_name,
                    "dagsterEvent": _input_for_dagster_event(event.dagster_event),
                }
            },
        )

    def delete_events(self, run_id: str):
        self._execute_query(
            DELETE_EVENTS_MUTATION, variables={"runId": check.str_param(run_id, "run_id")}
        )
        return

    def upgrade(self):
        return self._execute_query(UPGRADE_EVENT_LOG_STORAGE_MUTATION)

    def reindex_assets(self, print_fn: Callable = lambda _: None, force: bool = False):
        return self._execute_query(
            REINDEX_MUTATION, variables={"force": check.bool_param(force, "force")}
        )

    def reindex_events(self, print_fn: Callable = lambda _: None, force: bool = False):
        return self._execute_query(
            REINDEX_MUTATION, variables={"force": check.bool_param(force, "force")}
        )

    def wipe(self):
        return self._execute_query(WIPE_EVENT_LOG_STORAGE_MUTATION)

    def watch(self, run_id: str, start_cursor: int, callback: Callable):
        # If we wanted to implement this for real we would have to figure out how to pass the
        # callback as a serialized ConfigurableClass or equiv. May be better just to raise the
        # NotImplementedError in this class and remove the GQL endpoint completely
        return self._execute_query(
            WATCH_MUTATION,
            variables={
                "runId": check.str_param(run_id, "run_id"),
                "startCursor": check.int_param(start_cursor, "start_cursor"),
                "callback": r"{}",
            },
        )

    def end_watch(self, run_id: str, handler: Callable):
        return self._execute_query(
            END_WATCH_MUTATION,
            variables={"runId": check.str_param(run_id, "run_id"), "handler": r"{}"},
        )

    def enable_secondary_index(self, name: str, run_id: str = None):
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
                "assetKeys": [asset_key.to_string() for asset_key in asset_keys]
                if asset_keys
                else []
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

    def get_asset_events(
        self,
        asset_key: AssetKey,
        partitions: Optional[List[str]] = None,
        before_cursor: Optional[int] = None,
        after_cursor: Optional[int] = None,
        limit: Optional[int] = None,
        ascending: bool = False,
        include_cursor: bool = False,
        before_timestamp: Optional[float] = None,
        cursor: Optional[int] = None,
    ) -> Union[List[EventLogEntry], List[Tuple[int, EventLogEntry]]]:
        check.inst_param(asset_key, "asset_key", AssetKey)
        before_cursor = check.opt_int_param(before_cursor, "before_cursor")
        after_cursor = check.opt_int_param(after_cursor, "after_cursor")
        before_timestamp = check.opt_float_param(before_timestamp, "before_timestamp")
        cursor = check.opt_int_param(cursor, "cursor")

        before_cursor, after_cursor = extract_asset_events_cursor(
            cursor, before_cursor, after_cursor, ascending
        )

        event_records = self.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
                asset_partitions=partitions,
                before_cursor=before_cursor,
                after_cursor=after_cursor,
                before_timestamp=before_timestamp,
            ),
            limit=limit,
            ascending=ascending,
        )

        if include_cursor:
            return [(record.storage_id, record.event_log_entry) for record in event_records]
        else:
            return [record.event_log_entry for record in event_records]

    def get_asset_run_ids(self, asset_key: AssetKey) -> Iterable[str]:
        res = self._execute_query(
            GET_ASSET_RUN_IDS_QUERY, variables={"assetKey": asset_key.to_string()}
        )
        return res["data"]["eventLogs"]["getAssetRunIds"]

    def get_materialization_count_by_partition(
        self, asset_keys: Sequence[AssetKey]
    ) -> Mapping[AssetKey, Mapping[str, int]]:
        check.list_param(asset_keys, "asset_keys", of_type=AssetKey)

        res = self._execute_query(
            GET_MATERIALIZATION_COUNT_BY_PARTITION,
            variables={"assetKeys": [asset_key.to_string() for asset_key in asset_keys]},
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

    def wipe_asset(self, asset_key: AssetKey):
        res = self._execute_query(
            WIPE_ASSET_MUTATION, variables={"assetKey": asset_key.to_string()}
        )
        return res
