from typing import Dict, Optional

from dagster._core.secrets import SecretsLoader
from dagster._serdes import ConfigurableClass

SECRETS_QUERY = """
query Secrets($locationName: String) {
    scopedSecrets(locationName: $locationName) {
        secretName
        secretValue
        locationNames
    }
}
"""

from dagster_cloud_cli.core.errors import GraphQLStorageError


class DagsterCloudSecretsLoader(SecretsLoader, ConfigurableClass):
    def __init__(
        self,
        inst_data=None,
    ):
        self._inst_data = inst_data

    def _execute_query(self, query, variables=None):
        res = self._instance.graphql_client.execute(query, variable_values=variables)
        if "errors" in res:
            raise GraphQLStorageError(res)
        return res

    def get_secrets_for_environment(self, location_name: Optional[str]) -> Dict[str, str]:
        res = self._execute_query(
            SECRETS_QUERY,
            variables={"locationName": location_name},
        )

        secrets = res["data"]["scopedSecrets"]

        # Place secrets scoped to this location at the end so that they take priority over secrets
        # with the same name but no location scopes
        secrets = sorted(secrets, key=lambda secret: "1" if secret["locationNames"] else "0")

        return {secret["secretName"]: secret["secretValue"] for secret in secrets}

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return DagsterCloudSecretsLoader(inst_data=inst_data, **config_value)
