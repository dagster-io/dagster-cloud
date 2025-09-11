from typing import TYPE_CHECKING, Any, Optional

import dagster._check as check
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from typing_extensions import Self

from dagster_cloud.storage.defs_state.base_storage import BaseGraphQLDefsStateStorage

if TYPE_CHECKING:
    from dagster_cloud.instance import DagsterCloudAgentInstance  # noqa: F401


class GraphQLDefsStateStorage(
    BaseGraphQLDefsStateStorage["DagsterCloudAgentInstance"], ConfigurableClass
):
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
        return cls(inst_data=inst_data)

    @property
    def url(self) -> str:
        return self._instance.dagster_cloud_url

    @property
    def api_token(self) -> str:
        return check.not_none(self._instance.dagster_cloud_agent_token)

    @property
    def deployment(self) -> str:
        return check.not_none(self._instance.deployment_name)

    @property
    def graphql_client(self):
        return (
            self._override_graphql_client
            if self._override_graphql_client
            else self._instance.graphql_client  # pyright: ignore[reportAttributeAccessIssue]
        )
