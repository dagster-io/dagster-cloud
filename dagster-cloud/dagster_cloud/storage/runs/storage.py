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
)

import dagster._check as check
from dagster._core.errors import (
    DagsterInvariantViolationError,
    DagsterRunAlreadyExists,
    DagsterRunNotFoundError,
    DagsterSnapshotDoesNotExist,
)
from dagster._core.events import DagsterEvent
from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
from dagster._core.host_representation.origin import ExternalJobOrigin
from dagster._core.snap import (
    ExecutionPlanSnapshot,
    JobSnapshot,
    create_execution_plan_snapshot_id,
    create_job_snapshot_id,
)
from dagster._core.storage.dagster_run import (
    DagsterRun,
    JobBucket,
    RunPartitionData,
    RunRecord,
    RunsFilter,
    TagBucket,
)
from dagster._core.storage.runs.base import RunStorage
from dagster._daemon.types import DaemonHeartbeat
from dagster._serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    deserialize_value,
    serialize_value,
)
from dagster._utils import utc_datetime_from_timestamp
from dagster._utils.merger import merge_dicts
from dagster_cloud_cli.core.errors import GraphQLStorageError
from typing_extensions import Self

from .queries import (
    ADD_BACKFILL_MUTATION,
    ADD_DAEMON_HEARTBEAT_MUTATION,
    ADD_EXECUTION_PLAN_SNAPSHOT_MUTATION,
    ADD_PIPELINE_SNAPSHOT_MUTATION,
    ADD_RUN_MUTATION,
    ADD_RUN_TAGS_MUTATION,
    GET_BACKFILL_QUERY,
    GET_BACKFILLS_QUERY,
    GET_DAEMON_HEARTBEATS_QUERY,
    GET_EXECUTION_PLAN_SNAPSHOT_QUERY,
    GET_PIPELINE_SNAPSHOT_QUERY,
    GET_RUN_BY_ID_QUERY,
    GET_RUN_GROUP_QUERY,
    GET_RUN_IDS_QUERY,
    GET_RUN_PARTITION_DATA_QUERY,
    GET_RUN_RECORDS_QUERY,
    GET_RUN_TAG_KEYS_QUERY,
    GET_RUN_TAGS_QUERY,
    GET_RUNS_COUNT_QUERY,
    GET_RUNS_QUERY,
    HAS_EXECUTION_PLAN_SNAPSHOT_QUERY,
    HAS_PIPELINE_SNAPSHOT_QUERY,
    HAS_RUN_QUERY,
    MUTATE_JOB_ORIGIN,
    UPDATE_BACKFILL_MUTATION,
)


def _get_filters_input(filters: Optional[RunsFilter]) -> Optional[Dict[str, Any]]:
    filters = check.opt_inst_param(filters, "filters", RunsFilter)

    if filters is None:
        return None
    return {
        "runIds": filters.run_ids,
        "pipelineName": filters.job_name,
        "statuses": [status.value for status in filters.statuses],
        "tags": (
            [
                merge_dicts(
                    {"key": tag_key},
                    ({"value": tag_value} if isinstance(tag_value, str) else {"values": tag_value}),
                )
                for tag_key, tag_value in filters.tags.items()
            ]
            if filters.tags
            else []
        ),
        "snapshotId": filters.snapshot_id,
        "updatedAfter": filters.updated_after.timestamp() if filters.updated_after else None,
        "updatedBefore": filters.updated_before.timestamp() if filters.updated_before else None,
        "createdAfter": filters.created_after.timestamp() if filters.created_after else None,
        "createdBefore": filters.created_before.timestamp() if filters.created_before else None,
    }


def _run_record_from_graphql(graphene_run_record: Dict) -> RunRecord:
    check.dict_param(graphene_run_record, "graphene_run_record")
    return RunRecord(
        storage_id=check.int_elem(graphene_run_record, "storageId"),
        dagster_run=deserialize_value(
            check.str_elem(graphene_run_record, "serializedPipelineRun"),
            DagsterRun,
        ),
        create_timestamp=utc_datetime_from_timestamp(
            check.float_elem(graphene_run_record, "createTimestamp")
        ),
        update_timestamp=utc_datetime_from_timestamp(
            check.float_elem(graphene_run_record, "updateTimestamp")
        ),
        start_time=graphene_run_record.get("startTime"),
        end_time=graphene_run_record.get("endTime"),
    )


def _get_bucket_input(bucket_by: Optional[Union[JobBucket, TagBucket]]) -> Optional[Dict[str, Any]]:
    if not bucket_by:
        return None

    bucket_type = "job" if isinstance(bucket_by, JobBucket) else "tag"
    tag_key = bucket_by.tag_key if isinstance(bucket_by, TagBucket) else None
    values = bucket_by.job_names if isinstance(bucket_by, JobBucket) else bucket_by.tag_values
    return {
        "bucketType": bucket_type,
        "tagKey": tag_key,
        "values": values,
        "bucketLimit": bucket_by.bucket_limit,
    }


class GraphQLRunStorage(RunStorage, ConfigurableClass):
    def __init__(
        self, inst_data: Optional[ConfigurableClassData] = None, override_graphql_client=None
    ):
        """Initialize this class directly only for test (using `override_graphql_client`).
        Use the ConfigurableClass machinery to init from instance yaml.
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
        return GraphQLRunStorage(inst_data=inst_data)

    @property
    def _graphql_client(self):
        return (
            self._override_graphql_client
            if self._override_graphql_client
            else self._instance.graphql_client
        )

    def _execute_query(self, query, variables=None, idempotent_mutation=False):
        res = self._graphql_client.execute(
            query, variable_values=variables, idempotent_mutation=idempotent_mutation
        )
        if "errors" in res:
            raise GraphQLStorageError(res)
        return res

    def add_run(self, dagster_run: DagsterRun):
        check.inst_param(dagster_run, "dagster_run", DagsterRun)
        res = self._execute_query(
            ADD_RUN_MUTATION,
            variables={"serializedPipelineRun": serialize_value(dagster_run)},
        )

        result = res["data"]["runs"]["addRun"]
        error = result.get("error")

        # Special-case some errors to match the RunStorage API
        if error:
            if error["className"] == "DagsterRunAlreadyExists":
                raise DagsterRunAlreadyExists(error["message"])
            if error["className"] == "DagsterSnapshotDoesNotExist":
                raise DagsterSnapshotDoesNotExist(error["message"])
            else:
                raise GraphQLStorageError(res)

        return dagster_run

    def handle_run_event(self, run_id: str, event: DagsterEvent):
        # no-op, handled by store_event
        pass

    @property
    def supports_bucket_queries(self) -> bool:
        return False

    def get_runs(
        self,
        filters: Optional[RunsFilter] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        bucket_by: Optional[Union[JobBucket, TagBucket]] = None,
    ) -> Iterable[DagsterRun]:
        res = self._execute_query(
            GET_RUNS_QUERY,
            variables={
                "filters": _get_filters_input(filters),
                "cursor": check.opt_str_param(cursor, "cursor"),
                "limit": check.opt_int_param(limit, "limit"),
                "bucketBy": _get_bucket_input(bucket_by),
            },
        )
        return [deserialize_value(run, DagsterRun) for run in res["data"]["runs"]["getRuns"]]

    def get_run_ids(
        self,
        filters: Optional[RunsFilter] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Sequence[str]:
        res = self._execute_query(
            GET_RUN_IDS_QUERY,
            variables={
                "filters": _get_filters_input(filters),
                "cursor": check.opt_str_param(cursor, "cursor"),
                "limit": check.opt_int_param(limit, "limit"),
            },
        )
        return res["data"]["runs"]["getRunIds"]

    def get_runs_count(self, filters: Optional[RunsFilter] = None) -> int:
        res = self._execute_query(
            GET_RUNS_COUNT_QUERY,
            variables={
                "filters": _get_filters_input(filters),
            },
        )
        return res["data"]["runs"]["getRunsCount"]

    def get_run_group(self, run_id: str) -> Optional[Tuple[str, Iterable[DagsterRun]]]:
        res = self._execute_query(
            GET_RUN_GROUP_QUERY, variables={"runId": check.str_param(run_id, "run_id")}
        )
        run_group_or_error = res["data"]["runs"]["getRunGroupOrError"]
        if run_group_or_error["__typename"] == "SerializedRunGroup":
            return (
                run_group_or_error["rootRunId"],
                [
                    deserialize_value(run, DagsterRun)
                    for run in run_group_or_error["serializedRuns"]
                ],
            )
        elif run_group_or_error["__typename"] == "RunNotFoundError":
            raise DagsterRunNotFoundError(invalid_run_id=run_group_or_error["runId"])
        else:
            raise DagsterInvariantViolationError(f"Unexpected getRunGroupOrError response {res}")

    def get_run_by_id(self, run_id: str) -> Optional[DagsterRun]:
        res = self._execute_query(
            GET_RUN_BY_ID_QUERY, variables={"runId": check.str_param(run_id, "run_id")}
        )
        run = res["data"]["runs"]["getRunById"]
        if run is None:
            return None

        return deserialize_value(run, DagsterRun)

    def get_run_records(
        self,
        filters: Optional[RunsFilter] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        ascending: bool = False,
        cursor: Optional[str] = None,
        bucket_by: Optional[Union[JobBucket, TagBucket]] = None,
    ) -> List[RunRecord]:
        res = self._execute_query(
            GET_RUN_RECORDS_QUERY,
            variables={
                "filters": _get_filters_input(filters),
                "limit": check.opt_int_param(limit, "limit"),
                "orderBy": check.opt_str_param(order_by, "order_by"),
                "ascending": check.opt_bool_param(ascending, "ascending"),
                "cursor": check.opt_str_param(cursor, "cursor"),
                "bucketBy": _get_bucket_input(bucket_by),
            },
        )
        return [_run_record_from_graphql(record) for record in res["data"]["runs"]["getRunRecords"]]

    def get_run_tags(
        self,
        tag_keys: Optional[Sequence[str]] = None,
        value_prefix: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Sequence[Tuple[str, Set[str]]]:
        res = self._execute_query(
            GET_RUN_TAGS_QUERY,
            variables={
                "jsonTagKeys": (
                    json.dumps(check.list_param(tag_keys, "tag_keys", of_type=str))
                    if tag_keys
                    else None
                ),
                "valuePrefix": check.opt_str_param(value_prefix, "value_prefix"),
                "limit": check.opt_int_param(limit, "limit"),
            },
        )
        return [
            (run_tag["key"], set(run_tag["values"]))
            for run_tag in res["data"]["runs"]["getRunTags"]
        ]

    def get_run_tag_keys(self) -> Sequence[str]:
        res = self._execute_query(GET_RUN_TAG_KEYS_QUERY)
        return [run_tag_key for run_tag_key in res["data"]["runs"]["getRunTagKeys"]]

    def add_run_tags(self, run_id: str, new_tags: Mapping[str, str]):
        self._execute_query(
            ADD_RUN_TAGS_MUTATION,
            variables={
                "runId": check.str_param(run_id, "run_id"),
                "jsonNewTags": json.dumps(
                    check.mapping_param(new_tags, "new_tags", key_type=str, value_type=str)
                ),
            },
            idempotent_mutation=True,
        )

    def has_run(self, run_id: str) -> bool:
        res = self._execute_query(
            HAS_RUN_QUERY, variables={"runId": check.str_param(run_id, "run_id")}
        )
        return res["data"]["runs"]["hasRun"]

    def has_job_snapshot(self, pipeline_snapshot_id: str) -> bool:
        res = self._execute_query(
            HAS_PIPELINE_SNAPSHOT_QUERY,
            variables={
                "pipelineSnapshotId": check.str_param(pipeline_snapshot_id, "pipeline_snapshot_id")
            },
        )
        return res["data"]["runs"]["hasPipelineSnapshot"]

    def add_job_snapshot(
        self, pipeline_snapshot: JobSnapshot, snapshot_id: Optional[str] = None
    ) -> str:
        self._execute_query(
            ADD_PIPELINE_SNAPSHOT_MUTATION,
            variables={
                "serializedPipelineSnapshot": serialize_value(
                    check.inst_param(pipeline_snapshot, "pipeline_snapshot", JobSnapshot)
                ),
                "snapshotId": snapshot_id,
            },
        )
        return snapshot_id if snapshot_id else create_job_snapshot_id(pipeline_snapshot)

    def get_job_snapshot(self, pipeline_snapshot_id: str) -> JobSnapshot:
        res = self._execute_query(
            GET_PIPELINE_SNAPSHOT_QUERY,
            variables={
                "pipelineSnapshotId": check.str_param(pipeline_snapshot_id, "pipeline_snapshot_id")
            },
        )
        return deserialize_value(res["data"]["runs"]["getPipelineSnapshot"], JobSnapshot)

    def has_execution_plan_snapshot(self, execution_plan_snapshot_id: str) -> bool:
        res = self._execute_query(
            HAS_EXECUTION_PLAN_SNAPSHOT_QUERY,
            variables={
                "executionPlanSnapshotId": check.str_param(
                    execution_plan_snapshot_id, "execution_plan_snapshot_id"
                )
            },
        )
        return res["data"]["runs"]["hasExecutionPlanSnapshot"]

    def add_execution_plan_snapshot(
        self, execution_plan_snapshot: ExecutionPlanSnapshot, snapshot_id: Optional[str] = None
    ) -> str:
        self._execute_query(
            ADD_EXECUTION_PLAN_SNAPSHOT_MUTATION,
            variables={
                "serializedExecutionPlanSnapshot": serialize_value(
                    check.inst_param(
                        execution_plan_snapshot, "execution_plan_snapshot", ExecutionPlanSnapshot
                    )
                ),
                "snapshotId": snapshot_id,
            },
        )
        return (
            snapshot_id
            if snapshot_id
            else create_execution_plan_snapshot_id(execution_plan_snapshot)
        )

    def get_execution_plan_snapshot(self, execution_plan_snapshot_id: str) -> ExecutionPlanSnapshot:
        res = self._execute_query(
            GET_EXECUTION_PLAN_SNAPSHOT_QUERY,
            variables={
                "executionPlanSnapshotId": check.str_param(
                    execution_plan_snapshot_id, "execution_plan_snapshot_id"
                )
            },
        )
        return deserialize_value(
            res["data"]["runs"]["getExecutionPlanSnapshot"], ExecutionPlanSnapshot
        )

    def get_run_partition_data(self, runs_filter: RunsFilter) -> List[RunPartitionData]:
        res = self._execute_query(
            GET_RUN_PARTITION_DATA_QUERY,
            variables={
                "runsFilter": _get_filters_input(runs_filter),
            },
        )
        return [
            deserialize_value(result, RunPartitionData)
            for result in res["data"]["runs"]["getRunPartitionData"]
        ]

    def add_daemon_heartbeat(self, daemon_heartbeat: DaemonHeartbeat):
        self._execute_query(
            ADD_DAEMON_HEARTBEAT_MUTATION,
            variables={
                "serializedDaemonHeartbeat": serialize_value(
                    check.inst_param(daemon_heartbeat, "daemon_heartbeat", DaemonHeartbeat)
                )
            },
        )

    def build_missing_indexes(
        self, print_fn: Callable = lambda _: None, force_rebuild_all: bool = False
    ):
        raise Exception("Not allowed to build indexes from user cloud")

    def get_daemon_heartbeats(self) -> Dict[str, DaemonHeartbeat]:
        res = self._execute_query(GET_DAEMON_HEARTBEATS_QUERY)
        return {
            key: deserialize_value(heartbeat, DaemonHeartbeat)
            for key, heartbeat in json.loads(res["data"]["runs"]["getDaemonHeartbeats"]).items()
        }

    def delete_run(self, run_id: str):
        raise Exception("Not allowed to delete runs from user cloud")

    def wipe_daemon_heartbeats(self):
        raise Exception("Not allowed to wipe heartbeats from user cloud")

    def wipe(self):
        raise Exception("Not allowed to wipe runs from user cloud")

    def has_bulk_actions_table(self) -> bool:
        return False

    def get_backfills(
        self,
        status: Optional[BulkActionStatus] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        """Get a list of partition backfills."""
        res = self._execute_query(
            GET_BACKFILLS_QUERY,
            variables={
                "status": status.value if status else None,
                "cursor": check.opt_str_param(cursor, "cursor"),
                "limit": check.opt_int_param(limit, "limit"),
            },
        )
        return [
            deserialize_value(backfill, PartitionBackfill)
            for backfill in res["data"]["runs"]["getBackfills"]
        ]

    def get_backfill(self, backfill_id: str) -> PartitionBackfill:
        """Get a single partition backfill."""
        res = self._execute_query(GET_BACKFILL_QUERY, variables={"backfillId": backfill_id})
        backfill = res["data"]["runs"]["getBackfill"]
        return deserialize_value(backfill, PartitionBackfill)

    def add_backfill(self, partition_backfill: PartitionBackfill):
        """Add partition backfill to run storage."""
        self._execute_query(
            ADD_BACKFILL_MUTATION,
            variables={"serializedPartitionBackfill": serialize_value(partition_backfill)},
        )

    def update_backfill(self, partition_backfill: PartitionBackfill):
        """Update a partition backfill in run storage."""
        self._execute_query(
            UPDATE_BACKFILL_MUTATION,
            variables={"serializedPartitionBackfill": serialize_value(partition_backfill)},
        )

    def get_cursor_values(self, keys: Set[str]):
        return NotImplementedError("KVS is not supported from the user cloud")

    def set_cursor_values(self, pairs: Mapping[str, str]):
        return NotImplementedError("KVS is not supported from the user cloud")

    # Migrating run history
    def replace_job_origin(self, run: DagsterRun, job_origin: ExternalJobOrigin):
        self._execute_query(
            MUTATE_JOB_ORIGIN,
            variables={
                "runId": run.run_id,
                "serializedJobOrigin": serialize_value(job_origin),
            },
        )
