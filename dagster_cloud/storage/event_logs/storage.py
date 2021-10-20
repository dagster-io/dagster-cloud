import json
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Optional

from dagster import check
from dagster.core.definitions.events import AssetKey
from dagster.core.events import DagsterEvent, DagsterEventType
from dagster.core.events.log import EventLogEntry
from dagster.core.execution.stats import RunStepKeyStatsSnapshot, StepEventStatus
from dagster.core.storage.event_log.base import (
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
from dagster.utils import datetime_as_float
from dagster.utils.error import SerializableErrorInfo

from ..errors import GraphQLStorageError
from .queries import (
    DELETE_EVENTS_MUTATION,
    ENABLE_SECONDARY_INDEX_MUTATION,
    END_WATCH_MUTATION,
    GET_ALL_ASSET_KEYS_QUERY,
    GET_ALL_ASSET_TAGS_QUERY,
    GET_ASSET_RUN_IDS_QUERY,
    GET_ASSET_TAGS_QUERY,
    GET_EVENT_RECORDS_QUERY,
    GET_LOGS_FOR_RUN_QUERY,
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
        "message": serializable_error_info.mesage,
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
        message=check.str_elem(graphene_event_log_entry, "message"),
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
        "runUpdatedAfter": datetime_as_float(event_records_filter.after_cursor.run_updated_after)
        if isinstance(event_records_filter.after_cursor, RunShardedEventsCursor)
        else None,
    }


def _event_record_from_graphql(graphene_event_record: Dict) -> EventLogRecord:
    check.dict_param(graphene_event_record, "graphene_event_record")

    return EventLogRecord(
        storage_id=check.int_elem(graphene_event_record, "storageId"),
        event_log_entry=_event_log_entry_from_graphql(graphene_event_record["eventLogEntry"]),
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
        of_type: Optional[DagsterEventType] = None,
        limit: Optional[int] = None,
    ) -> Iterable[EventLogEntry]:
        check.opt_inst_param(of_type, "of_type", DagsterEventType)
        res = self._execute_query(
            GET_LOGS_FOR_RUN_QUERY,
            variables={
                "runId": check.str_param(run_id, "run_id"),
                "cursor": check.int_param(cursor, "cursor"),
                "ofType": of_type.value if of_type else None,
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
                    getattr(StepEventStatus, check.opt_str_elem(stats, "status"))
                    if stats.get("status") is not None
                    else None
                ),
                start_time=check.opt_float_elem(stats, "startTime"),
                end_time=check.opt_float_elem(stats, "endTime"),
                materializations=[
                    deserialize_json_to_dagster_namedtuple(materialization)
                    for materialization in check.opt_list_elem(
                        stats, "materializations", of_type=str
                    )
                ],
                expectation_results=[
                    deserialize_json_to_dagster_namedtuple(expectation_result)
                    for expectation_result in check.opt_list_elem(
                        stats, "expectationResults", of_type=str
                    )
                ],
                attempts=check.opt_int_elem(stats, "attempts"),
            )
            for stats in step_stats
        ]

    def store_event(self, event: EventLogEntry):
        check.inst_param(event, "event", EventLogEntry)

        self._execute_query(
            STORE_EVENT_MUTATION,
            variables={
                "eventRecord": {
                    "errorInfo": _input_for_serializable_error_info(event.error_info),
                    "message": event.message,
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

    def has_asset_key(self, asset_key: AssetKey):
        check.inst_param(asset_key, "asset_key", AssetKey)

        res = self._execute_query(
            HAS_ASSET_KEY_QUERY, variables={"assetKey": asset_key.to_string()}
        )
        return res["data"]["eventLogs"]["hasAssetKey"]

    def all_asset_keys(self) -> Iterable[AssetKey]:
        res = self._execute_query(GET_ALL_ASSET_KEYS_QUERY)
        return [
            AssetKey.from_db_string(asset_key_string)
            for asset_key_string in res["data"]["eventLogs"]["getAllAssetKeys"]
        ]

    def all_asset_tags(self) -> Dict[AssetKey, Dict[str, str]]:
        res = self._execute_query(GET_ALL_ASSET_TAGS_QUERY)
        asset_tags: Dict[AssetKey, Dict[str, str]] = defaultdict(dict)
        for asset_tag_entry in res["data"]["eventLogs"]["getAllAssetTags"]:
            asset_key_string = asset_tag_entry["assetKey"]
            tag_key = asset_tag_entry["tagKey"]
            tag_value = asset_tag_entry["tagValue"]
            asset_tags[AssetKey.from_db_string(asset_key_string)][tag_key] = tag_value

        return asset_tags

    def get_asset_tags(self, asset_key: AssetKey) -> Dict[str, str]:
        res = self._execute_query(
            GET_ASSET_TAGS_QUERY, variables={"assetKey": asset_key.to_string()}
        )
        return {entry["key"]: entry["value"] for entry in res["data"]["eventLogs"]["getAssetTags"]}

    def get_asset_events(
        self,
        asset_key: AssetKey,
        partitions: List[str] = None,
        before_cursor: int = None,
        after_cursor: int = None,
        limit: int = None,
        ascending: bool = False,
        include_cursor: bool = False,
        before_timestamp: float = None,
        cursor: int = None,
    ) -> Iterable[EventLogEntry]:
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
            return [tuple([record.storage_id, record.event_log_entry]) for record in event_records]
        else:
            return [record.event_log_entry for record in event_records]

    def get_asset_run_ids(self, asset_key: AssetKey) -> Iterable[str]:
        res = self._execute_query(
            GET_ASSET_RUN_IDS_QUERY, variables={"assetKey": asset_key.to_string()}
        )
        return res["data"]["eventLogs"]["getAssetRunIds"]

    def wipe_asset(self, asset_key: AssetKey):
        res = self._execute_query(
            WIPE_ASSET_MUTATION, variables={"assetKey": asset_key.to_string()}
        )
        return res
