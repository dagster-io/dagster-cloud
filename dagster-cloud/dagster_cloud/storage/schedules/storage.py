from typing import Iterable

from dagster import check
from dagster.core.definitions.run_request import InstigatorType
from dagster.core.scheduler.instigation import (
    InstigatorState,
    InstigatorTick,
    TickData,
    TickStatsSnapshot,
    TickStatus,
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

    def all_instigator_state(
        self, repository_origin_id: str = None, instigator_type: InstigatorType = None
    ) -> Iterable[InstigatorState]:
        res = self._execute_query(
            ALL_STORED_JOB_STATE_QUERY,
            variables={
                "repositoryOriginId": repository_origin_id,
                "jobType": instigator_type.value if instigator_type else None,
            },
        )

        return [
            deserialize_as(state, InstigatorState)
            for state in res["data"]["schedules"]["jobStates"]
        ]

    def get_instigator_state(self, origin_id: str) -> InstigatorState:
        res = self._execute_query(
            GET_JOB_STATE_QUERY,
            variables={
                "jobOriginId": origin_id,
            },
        )

        state = res["data"]["schedules"]["jobState"]
        if state is None:
            return None

        return deserialize_as(state, InstigatorState)

    def add_instigator_state(self, state: InstigatorState):
        self._execute_query(
            ADD_JOB_STATE_MUTATION,
            variables={
                "serializedJobState": serialize_dagster_namedtuple(
                    check.inst_param(state, "state", InstigatorState)
                )
            },
        )

    def update_instigator_state(self, state: InstigatorState):
        self._execute_query(
            UPDATE_JOB_STATE_MUTATION,
            variables={
                "serializedJobState": serialize_dagster_namedtuple(
                    check.inst_param(state, "state", InstigatorState)
                )
            },
        )

    def delete_instigator_state(self, origin_id: str):
        raise NotImplementedError("Not callable from user cloud")

    def get_ticks(
        self, origin_id: str, before: float = None, after: float = None, limit: int = None
    ) -> Iterable[InstigatorTick]:
        res = self._execute_query(
            GET_JOB_TICKS_QUERY,
            variables={
                "jobOriginId": origin_id,
                "before": before,
                "after": after,
                "limit": limit,
            },
        )

        return [
            deserialize_as(tick, InstigatorTick) for tick in res["data"]["schedules"]["jobTicks"]
        ]

    def create_tick(self, tick_data: TickData):
        res = self._execute_query(
            CREATE_JOB_TICK_MUTATION,
            variables={
                "serializedJobTickData": serialize_dagster_namedtuple(
                    check.inst_param(tick_data, "tick_data", TickData)
                )
            },
        )

        tick_id = res["data"]["schedules"]["createJobTick"]["tickId"]
        return InstigatorTick(tick_id, tick_data)

    def update_tick(self, tick: InstigatorTick):
        self._execute_query(
            UPDATE_JOB_TICK_MUTATION,
            variables={
                "tickId": tick.tick_id,
                "serializedJobTickData": serialize_dagster_namedtuple(
                    check.inst_param(tick.tick_data, "tick_data", TickData)
                ),
            },
        )
        return tick

    def purge_ticks(self, origin_id: str, tick_status: TickStatus, before: float):
        self._execute_query(
            PURGE_JOB_TICKS_MUTATION,
            variables={
                "jobOriginId": origin_id,
                "tickStatus": tick_status.value if tick_status else None,
                "before": before,
            },
        )

    def get_tick_stats(self, origin_id: str):
        res = self._execute_query(
            GET_JOB_TICK_STATS_QUERY,
            variables={
                "jobOriginId": origin_id,
            },
        )

        stats = res["data"]["schedules"]["jobTickStats"]
        return TickStatsSnapshot(
            ticks_started=stats["ticksStarted"],
            ticks_succeeded=stats["ticksSucceeded"],
            ticks_skipped=stats["ticksSkipped"],
            ticks_failed=stats["ticksFailed"],
        )

    def upgrade(self):
        raise NotImplementedError("Not callable from user cloud")

    def optimize_for_dagit(self, statement_timeout: int):
        raise NotImplementedError("Not callable from user cloud")
