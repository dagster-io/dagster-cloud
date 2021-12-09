from typing import Iterable

from dagster import check
from dagster.core.definitions.run_request import JobType
from dagster.core.scheduler.job import (
    JobState,
    JobTick,
    JobTickData,
    JobTickStatsSnapshot,
    JobTickStatus,
)
from dagster.core.storage.schedules.base import ScheduleStorage
from dagster.serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    deserialize_as,
    serialize_dagster_namedtuple,
)

from ..errors import GraphQLStorageError
from .queries import (
    ADD_JOB_STATE_MUTATION,
    ALL_STORED_JOB_STATE_QUERY,
    CREATE_JOB_TICK_MUTATION,
    GET_JOB_STATE_QUERY,
    GET_JOB_TICKS_QUERY,
    GET_JOB_TICK_STATS_QUERY,
    LATEST_JOB_TICK_QUERY,
    PURGE_JOB_TICKS_MUTATION,
    UPDATE_JOB_STATE_MUTATION,
    UPDATE_JOB_TICK_MUTATION,
)


class GraphQLScheduleStorage(ScheduleStorage, ConfigurableClass):
    def __init__(self, inst_data=None, override_graphql_client=None):
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
        return GraphQLScheduleStorage(inst_data=inst_data)

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

    def wipe(self):
        raise Exception("Not allowed to wipe from user cloud")

    def all_stored_job_state(
        self, repository_origin_id: str = None, job_type: JobType = None
    ) -> Iterable[JobState]:
        res = self._execute_query(
            ALL_STORED_JOB_STATE_QUERY,
            variables={
                "repositoryOriginId": repository_origin_id,
                "jobType": job_type.value if job_type else None,
            },
        )

        return [
            deserialize_as(job_state, JobState)
            for job_state in res["data"]["schedules"]["jobStates"]
        ]

    def get_job_state(self, job_origin_id: str) -> JobState:
        res = self._execute_query(
            GET_JOB_STATE_QUERY,
            variables={
                "jobOriginId": job_origin_id,
            },
        )

        job_state = res["data"]["schedules"]["jobState"]
        if job_state is None:
            return None

        return deserialize_as(job_state, JobState)

    def add_job_state(self, job: JobState):
        self._execute_query(
            ADD_JOB_STATE_MUTATION,
            variables={
                "serializedJobState": serialize_dagster_namedtuple(
                    check.inst_param(job, "job", JobState)
                )
            },
        )

    def update_job_state(self, job: JobState):
        self._execute_query(
            UPDATE_JOB_STATE_MUTATION,
            variables={
                "serializedJobState": serialize_dagster_namedtuple(
                    check.inst_param(job, "job", JobState)
                )
            },
        )

    def delete_job_state(self, job_origin_id: str):
        raise NotImplementedError("Not callable from user cloud")

    def get_job_ticks(
        self, job_origin_id: str, before: float = None, after: float = None, limit: int = None
    ) -> Iterable[JobTick]:
        res = self._execute_query(
            GET_JOB_TICKS_QUERY,
            variables={
                "jobOriginId": job_origin_id,
                "before": before,
                "after": after,
                "limit": limit,
            },
        )

        return [
            deserialize_as(job_tick, JobTick) for job_tick in res["data"]["schedules"]["jobTicks"]
        ]

    def get_latest_job_tick(self, job_origin_id: str) -> JobTick:
        res = self._execute_query(
            LATEST_JOB_TICK_QUERY,
            variables={
                "jobOriginId": job_origin_id,
            },
        )

        latest_tick = res["data"]["schedules"]["latestJobTick"]
        if not latest_tick:
            return None

        return deserialize_as(latest_tick, JobTick)

    def create_job_tick(self, job_tick_data: JobTickData):
        res = self._execute_query(
            CREATE_JOB_TICK_MUTATION,
            variables={
                "serializedJobTickData": serialize_dagster_namedtuple(
                    check.inst_param(job_tick_data, "job_tick_data", JobTickData)
                )
            },
        )

        tick_id = res["data"]["schedules"]["createJobTick"]["tickId"]
        return JobTick(tick_id, job_tick_data)

    def update_job_tick(self, tick: JobTick):
        self._execute_query(
            UPDATE_JOB_TICK_MUTATION,
            variables={
                "tickId": tick.tick_id,
                "serializedJobTickData": serialize_dagster_namedtuple(
                    check.inst_param(tick.job_tick_data, "job_tick_data", JobTickData)
                ),
            },
        )
        return tick

    def purge_job_ticks(self, job_origin_id: str, tick_status: JobTickStatus, before: float):
        self._execute_query(
            PURGE_JOB_TICKS_MUTATION,
            variables={
                "jobOriginId": job_origin_id,
                "tickStatus": tick_status.value if tick_status else None,
                "before": before,
            },
        )

    def get_job_tick_stats(self, job_origin_id: str):
        res = self._execute_query(
            GET_JOB_TICK_STATS_QUERY,
            variables={
                "jobOriginId": job_origin_id,
            },
        )

        stats = res["data"]["schedules"]["jobTickStats"]
        return JobTickStatsSnapshot(
            ticks_started=stats["ticksStarted"],
            ticks_succeeded=stats["ticksSucceeded"],
            ticks_skipped=stats["ticksSkipped"],
            ticks_failed=stats["ticksFailed"],
        )

    def upgrade(self):
        raise NotImplementedError("Not callable from user cloud")

    def optimize_for_dagit(self, statement_timeout: int):
        raise NotImplementedError("Not callable from user cloud")
