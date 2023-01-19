from typing import Iterable, Optional, Sequence

import dagster._check as check
from dagster._core.definitions.run_request import InstigatorType
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorTick,
    TickData,
    TickStatus,
)
from dagster._core.storage.schedules.base import ScheduleStorage
from dagster._serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    deserialize_as,
    serialize_dagster_namedtuple,
)
from dagster_cloud_cli.core.errors import GraphQLStorageError

from .queries import (
    ADD_JOB_STATE_MUTATION,
    ALL_STORED_JOB_STATE_QUERY,
    CREATE_JOB_TICK_MUTATION,
    GET_JOB_STATE_QUERY,
    GET_JOB_TICKS_QUERY,
    UPDATE_JOB_STATE_MUTATION,
    UPDATE_JOB_TICK_MUTATION,
)


class GraphQLScheduleStorage(ScheduleStorage, ConfigurableClass):
    def __init__(self, inst_data=None, override_graphql_client=None):
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
        self,
        repository_origin_id: Optional[str] = None,
        repository_selector_id: Optional[str] = None,
        instigator_type: Optional[InstigatorType] = None,
    ) -> Iterable[InstigatorState]:
        res = self._execute_query(
            ALL_STORED_JOB_STATE_QUERY,
            variables={
                "repositoryOriginId": repository_origin_id,
                "repositorySelectorId": repository_selector_id,
                "jobType": instigator_type.value if instigator_type else None,
            },
        )

        return [
            deserialize_as(state, InstigatorState)
            for state in res["data"]["schedules"]["jobStates"]
        ]

    def get_instigator_state(self, origin_id: str, selector_id: str) -> Optional[InstigatorState]:
        res = self._execute_query(
            GET_JOB_STATE_QUERY,
            variables={
                "jobOriginId": origin_id,
                "selectorId": selector_id,
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

    def delete_instigator_state(self, origin_id: str, selector_id: str):
        raise NotImplementedError("Not callable from user cloud")

    def get_ticks(
        self,
        origin_id: str,
        selector_id: str,
        before: Optional[float] = None,
        after: Optional[float] = None,
        limit: Optional[int] = None,
        statuses: Optional[Sequence[TickStatus]] = None,
    ) -> Iterable[InstigatorTick]:
        statuses = [status.value for status in statuses] if statuses else None
        res = self._execute_query(
            GET_JOB_TICKS_QUERY,
            variables={
                "jobOriginId": origin_id,
                "selectorId": selector_id,
                "before": before,
                "after": after,
                "limit": limit,
                "statuses": statuses,
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

    def purge_ticks(
        self,
        origin_id: str,
        selector_id: str,
        before: float,
        tick_statuses: Optional[Sequence[TickStatus]] = None,
    ):
        raise NotImplementedError("Not callable from user cloud")

    def upgrade(self):
        raise NotImplementedError("Not callable from user cloud")

    def optimize_for_dagit(self, statement_timeout: int, pool_recycle: int):
        raise NotImplementedError("Not callable from user cloud")
