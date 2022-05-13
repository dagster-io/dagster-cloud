import json
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

import dagster._check as check
from dagster.core.errors import (
    DagsterInvariantViolationError,
    DagsterRunAlreadyExists,
    DagsterRunNotFoundError,
    DagsterSnapshotDoesNotExist,
)
from dagster.core.events import DagsterEvent
from dagster.core.execution.backfill import BulkActionStatus, PartitionBackfill
from dagster.core.snap import (
    ExecutionPlanSnapshot,
    PipelineSnapshot,
    create_execution_plan_snapshot_id,
    create_pipeline_snapshot_id,
)
from dagster.core.storage.pipeline_run import (
    JobBucket,
    PipelineRun,
    PipelineRunsFilter,
    RunPartitionData,
    RunRecord,
    TagBucket,
)
from dagster.core.storage.runs.base import RunStorage
from dagster.daemon.types import DaemonHeartbeat
from dagster.serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    deserialize_as,
    deserialize_json_to_dagster_namedtuple,
    serialize_dagster_namedtuple,
)
from dagster.utils import utc_datetime_from_timestamp

from ..errors import GraphQLStorageError
from .queries import (
    ADD_BACKFILL_MUTATION,
    ADD_DAEMON_HEARTBEAT_MUTATION,
    ADD_EXECUTION_PLAN_SNAPSHOT_MUTATION,
    ADD_PIPELINE_SNAPSHOT_MUTATION,
    ADD_RUN_MUTATION,
    ADD_RUN_TAGS_MUTATION,
    GET_BACKFILLS_QUERY,
    GET_BACKFILL_QUERY,
    GET_DAEMON_HEARTBEATS_QUERY,
    GET_EXECUTION_PLAN_SNAPSHOT_QUERY,
    GET_PIPELINE_SNAPSHOT_QUERY,
    GET_RUNS_COUNT_QUERY,
    GET_RUNS_QUERY,
    GET_RUN_BY_ID_QUERY,
    GET_RUN_GROUPS_QUERY,
    GET_RUN_GROUP_QUERY,
    GET_RUN_PARTITION_DATA_QUERY,
    GET_RUN_RECORDS_QUERY,
    GET_RUN_TAGS_QUERY,
    HANDLE_RUN_EVENT_MUTATION,
    HAS_EXECUTION_PLAN_SNAPSHOT_QUERY,
    HAS_PIPELINE_SNAPSHOT_QUERY,
    HAS_RUN_QUERY,
    UPDATE_BACKFILL_MUTATION,
)


def _get_filters_input(filters) -> Optional[Dict[str, Any]]:
    filters = check.opt_inst_param(filters, "filters", PipelineRunsFilter)

    if filters is None:
        return None
    return {
        "runIds": filters.run_ids,
        "pipelineName": filters.pipeline_name,
        "mode": filters.mode,
        "statuses": [status.value for status in filters.statuses],
        "tags": [
            {"key": tag_key, "value": tag_value} for tag_key, tag_value in filters.tags.items()
        ]
        if filters.tags
        else [],
        "snapshotId": filters.snapshot_id,
        "updatedAfter": filters.updated_after.timestamp() if filters.updated_after else None,
        "createdBefore": filters.created_before.timestamp() if filters.created_before else None,
    }


def _run_record_from_graphql(graphene_run_record: Dict) -> RunRecord:
    check.dict_param(graphene_run_record, "graphene_run_record")
    return RunRecord(
        storage_id=check.int_elem(graphene_run_record, "storageId"),
        pipeline_run=check.inst(
            deserialize_json_to_dagster_namedtuple(
                check.str_elem(graphene_run_record, "serializedPipelineRun")
            ),
            PipelineRun,
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
    def __init__(self, inst_data: ConfigurableClassData = None, override_graphql_client=None):
        """Initialize this class directly only for test (using `override_graphql_client`).
        Use the ConfigurableClass machinery to init from instance yaml."""
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
        return GraphQLRunStorage(inst_data=inst_data)

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

    def add_run(self, pipeline_run: PipelineRun):
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        res = self._execute_query(
            ADD_RUN_MUTATION,
            variables={"serializedPipelineRun": serialize_dagster_namedtuple(pipeline_run)},
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

        return pipeline_run

    def handle_run_event(self, run_id: str, event: DagsterEvent):
        check.inst_param(event, "event", DagsterEvent)
        self._execute_query(
            HANDLE_RUN_EVENT_MUTATION,
            variables={
                "runId": check.str_param(run_id, "run_id"),
                "serializedEvent": serialize_dagster_namedtuple(event),
            },
        )

    def get_runs(
        self,
        filters: PipelineRunsFilter = None,
        cursor: str = None,
        limit: int = None,
        bucket_by: Optional[Union[JobBucket, TagBucket]] = None,
    ) -> Iterable[PipelineRun]:
        res = self._execute_query(
            GET_RUNS_QUERY,
            variables={
                "filters": _get_filters_input(filters),
                "cursor": check.opt_str_param(cursor, "cursor"),
                "limit": check.opt_int_param(limit, "limit"),
                "bucketBy": _get_bucket_input(bucket_by),
            },
        )
        return [deserialize_as(run, PipelineRun) for run in res["data"]["runs"]["getRuns"]]

    def get_runs_count(self, filters: PipelineRunsFilter = None) -> int:
        res = self._execute_query(
            GET_RUNS_COUNT_QUERY,
            variables={
                "filters": _get_filters_input(filters),
            },
        )
        return res["data"]["runs"]["getRunsCount"]

    def get_run_group(self, run_id: str) -> Optional[Tuple[str, Iterable[PipelineRun]]]:
        res = self._execute_query(
            GET_RUN_GROUP_QUERY, variables={"runId": check.str_param(run_id, "run_id")}
        )
        run_group_or_error = res["data"]["runs"]["getRunGroupOrError"]
        if run_group_or_error["__typename"] == "SerializedRunGroup":
            return (
                run_group_or_error["rootRunId"],
                [deserialize_as(run, PipelineRun) for run in run_group_or_error["serializedRuns"]],
            )
        elif run_group_or_error["__typename"] == "RunNotFoundError":
            raise DagsterRunNotFoundError(invalid_run_id=run_group_or_error["runId"])
        else:
            raise DagsterInvariantViolationError(
                f"Unexpected getRunGroupOrError response {str(res)}"
            )

    def get_run_groups(
        self, filters: PipelineRunsFilter = None, cursor: str = None, limit: int = None
    ) -> Dict[str, Dict[str, Union[Iterable[PipelineRun], int]]]:
        res = self._execute_query(
            GET_RUN_GROUPS_QUERY,
            variables={
                "filters": _get_filters_input(filters),
                "cursor": check.opt_str_param(cursor, "cursor"),
                "limit": check.opt_int_param(limit, "limit"),
            },
        )
        raw_run_groups = res["data"]["runs"]["getRunGroups"]

        run_groups = {}
        for run_group in raw_run_groups:
            run_groups[run_group["rootRunId"]] = {
                "runs": [deserialize_as(run, PipelineRun) for run in run_group["serializedRuns"]],
                "count": run_group["count"],
            }
        return run_groups

    def get_run_by_id(self, run_id: str) -> Optional[PipelineRun]:
        res = self._execute_query(
            GET_RUN_BY_ID_QUERY, variables={"runId": check.str_param(run_id, "run_id")}
        )
        run = res["data"]["runs"]["getRunById"]
        if run is None:
            return None

        return deserialize_as(run, PipelineRun)

    def get_run_records(
        self,
        filters: PipelineRunsFilter = None,
        limit: int = None,
        order_by: str = None,
        ascending: bool = False,
        cursor: str = None,
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

    def get_run_tags(self) -> List[Tuple[str, Set[str]]]:
        res = self._execute_query(GET_RUN_TAGS_QUERY)
        return [
            (run_tag["key"], set(run_tag["values"]))
            for run_tag in res["data"]["runs"]["getRunTags"]
        ]

    def add_run_tags(self, run_id: str, new_tags: Dict[str, str]):
        self._execute_query(
            ADD_RUN_TAGS_MUTATION,
            variables={
                "runId": check.str_param(run_id, "run_id"),
                "jsonNewTags": json.dumps(
                    check.dict_param(new_tags, "new_tags", key_type=str, value_type=str)
                ),
            },
        )

    def has_run(self, run_id: str) -> bool:
        res = self._execute_query(
            HAS_RUN_QUERY, variables={"runId": check.str_param(run_id, "run_id")}
        )
        return res["data"]["runs"]["hasRun"]

    def has_pipeline_snapshot(self, pipeline_snapshot_id: str) -> bool:
        res = self._execute_query(
            HAS_PIPELINE_SNAPSHOT_QUERY,
            variables={
                "pipelineSnapshotId": check.str_param(pipeline_snapshot_id, "pipeline_snapshot_id")
            },
        )
        return res["data"]["runs"]["hasPipelineSnapshot"]

    def add_pipeline_snapshot(
        self, pipeline_snapshot: PipelineSnapshot, snapshot_id: Optional[str] = None
    ) -> str:
        self._execute_query(
            ADD_PIPELINE_SNAPSHOT_MUTATION,
            variables={
                "serializedPipelineSnapshot": serialize_dagster_namedtuple(
                    check.inst_param(pipeline_snapshot, "pipeline_snapshot", PipelineSnapshot)
                ),
                "snapshotId": snapshot_id,
            },
        )
        return snapshot_id if snapshot_id else create_pipeline_snapshot_id(pipeline_snapshot)

    def get_pipeline_snapshot(self, pipeline_snapshot_id: str) -> PipelineSnapshot:
        res = self._execute_query(
            GET_PIPELINE_SNAPSHOT_QUERY,
            variables={
                "pipelineSnapshotId": check.str_param(pipeline_snapshot_id, "pipeline_snapshot_id")
            },
        )
        return deserialize_as(res["data"]["runs"]["getPipelineSnapshot"], PipelineSnapshot)

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
                "serializedExecutionPlanSnapshot": serialize_dagster_namedtuple(
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
        return deserialize_as(
            res["data"]["runs"]["getExecutionPlanSnapshot"], ExecutionPlanSnapshot
        )

    def get_run_partition_data(
        self, partition_set_name: str, job_name: str, repository_label: str
    ) -> List[RunPartitionData]:
        res = self._execute_query(
            GET_RUN_PARTITION_DATA_QUERY,
            variables={
                "partitionSetName": check.str_param(partition_set_name, "partition_set_name"),
                "jobName": check.str_param(job_name, "job_name"),
                "repositoryLabel": check.str_param(repository_label, "repository_label"),
            },
        )
        return [
            deserialize_as(result, RunPartitionData)
            for result in res["data"]["runs"]["getRunPartitionData"]
        ]

    def add_daemon_heartbeat(self, daemon_heartbeat: DaemonHeartbeat):
        self._execute_query(
            ADD_DAEMON_HEARTBEAT_MUTATION,
            variables={
                "serializedDaemonHeartbeat": serialize_dagster_namedtuple(
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
            key: deserialize_as(heartbeat, DaemonHeartbeat)
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

    def get_backfills(self, status: BulkActionStatus = None, cursor: str = None, limit: int = None):
        """Get a list of partition backfills"""
        res = self._execute_query(
            GET_BACKFILLS_QUERY,
            variables={
                "status": status.value if status else None,
                "cursor": check.opt_str_param(cursor, "cursor"),
                "limit": check.opt_int_param(limit, "limit"),
            },
        )
        return [
            deserialize_as(backfill, PartitionBackfill)
            for backfill in res["data"]["runs"]["getBackfills"]
        ]

    def get_backfill(self, backfill_id: str) -> PartitionBackfill:
        """Get a single partition backfill"""
        res = self._execute_query(GET_BACKFILL_QUERY, variables={"backfillId": backfill_id})
        backfill = res["data"]["runs"]["getBackfill"]
        return deserialize_as(backfill, PartitionBackfill)

    def add_backfill(self, partition_backfill: PartitionBackfill):
        """Add partition backfill to run storage"""
        self._execute_query(
            ADD_BACKFILL_MUTATION,
            variables={
                "serializedPartitionBackfill": serialize_dagster_namedtuple(partition_backfill)
            },
        )

    def update_backfill(self, partition_backfill: PartitionBackfill):
        """Update a partition backfill in run storage"""
        self._execute_query(
            UPDATE_BACKFILL_MUTATION,
            variables={
                "serializedPartitionBackfill": serialize_dagster_namedtuple(partition_backfill)
            },
        )
