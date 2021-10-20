from contextlib import ExitStack

import yaml
from dagster import Field, check
from dagster.config.validate import process_config, resolve_to_config_type
from dagster.core.errors import DagsterInvalidConfigError, DagsterInvariantViolationError
from dagster.core.instance import DagsterInstance
from dagster.core.instance.config import config_field_for_configurable_class
from dagster.core.instance.ref import ConfigurableClassData, InstanceRef, configurable_class_data
from dagster.core.launcher import RunLauncher
from dagster.utils import merge_dicts

from ..storage.client import create_proxy_client, dagster_cloud_api_config, get_agent_headers


class DagsterCloudInstance(DagsterInstance):
    pass


class DagsterCloudAgentInstance(DagsterCloudInstance):
    def __init__(self, *args, dagster_cloud_api, user_code_launcher=None, **kwargs):
        super().__init__(*args, **kwargs)

        processed_api_config = process_config(
            resolve_to_config_type(dagster_cloud_api_config()),
            check.dict_param(dagster_cloud_api, "dagster_cloud_api"),
        )

        if not processed_api_config.success:
            raise DagsterInvalidConfigError(
                "Errors whilst loading dagster_cloud_api config",
                processed_api_config.errors,
                dagster_cloud_api,
            )

        self._dagster_cloud_api_config = processed_api_config.value

        self._user_code_launcher_data = (
            configurable_class_data(user_code_launcher) if user_code_launcher else None
        )

        if not self._user_code_launcher_data:
            # This is a user facing error. We should have more actionable advice and link to docs here.
            raise DagsterInvariantViolationError(
                "User code launcher is not configured for DagsterCloudAgentInstance. "
                "Configure a user code launcher under the user_code_launcher: key in your dagster.yaml file."
            )

        self._exit_stack = ExitStack()

        self._user_code_launcher = None

        self._graphql_client = None

    def create_graphql_client(self):
        return create_proxy_client(self.dagster_cloud_graphql_url, self._dagster_cloud_api_config)

    @property
    def graphql_client(self):
        if self._graphql_client == None:
            self._graphql_client = self._exit_stack.enter_context(self.create_graphql_client())

        return self._graphql_client

    @property
    def dagster_cloud_url(self):
        return self._dagster_cloud_api_config["url"]

    @property
    def dagster_cloud_graphql_url(self):
        return f"{self.dagster_cloud_url}/graphql"

    @property
    def dagster_cloud_upload_logs_url(self):
        return f"{self.dagster_cloud_url}/upload_logs"

    @property
    def dagster_cloud_upload_workspace_entry_url(self):
        return f"{self.dagster_cloud_url}/upload_workspace_entry"

    @property
    def dagster_cloud_username(self):
        return self._dagster_cloud_api_config.get("username")

    @property
    def dagster_cloud_password(self):
        return self._dagster_cloud_api_config.get("password")

    @property
    def dagster_cloud_api_headers(self):
        return get_agent_headers(self._dagster_cloud_api_config)

    @property
    def dagster_cloud_agent_token(self):
        return self._dagster_cloud_api_config.get("agent_token")

    @property
    def user_code_launcher(self):
        # Lazily load in case the user code launcher requires dependencies (like dagster-k8s)
        # that we don't neccesarily need to load in every context that loads a
        # DagsterCloudAgentInstance (for example, a step worker)
        if not self._user_code_launcher:
            self._user_code_launcher = self._exit_stack.enter_context(
                self._user_code_launcher_data.rehydrate()
            )
            self._user_code_launcher.register_instance(self)
        return self._user_code_launcher

    @property
    def run_launcher(self) -> RunLauncher:
        # Run launcher is determined by the user code launcher
        return self.user_code_launcher.run_launcher()

    @staticmethod
    def get():  # pylint: disable=arguments-differ
        instance = DagsterInstance.get()
        if not isinstance(instance, DagsterCloudAgentInstance):
            raise DagsterInvariantViolationError(
                """
DagsterInstance.get() did not return a DagsterCloudAgentInstance. Make sure that your"
`dagster.yaml` file is correctly configured to include the following:
instance_class:
  module: dagster_cloud.instance
  class: DagsterCloudAgentInstance
"""
            )
        return instance

    @classmethod
    def config_schema(cls):
        return {
            "dagster_cloud_api": Field(dagster_cloud_api_config(), is_required=True),
            "user_code_launcher": config_field_for_configurable_class(),
        }

    def get_required_daemon_types(self):
        from dagster_cloud.daemon.dagster_cloud_api_daemon import DagsterCloudApiDaemon

        return [DagsterCloudApiDaemon.daemon_type()]

    @staticmethod
    def config_defaults(base_dir):
        defaults = InstanceRef.config_defaults(base_dir)

        empty_yaml = yaml.dump({})

        defaults["run_storage"] = ConfigurableClassData(
            "dagster_cloud.storage.runs",
            "GraphQLRunStorage",
            empty_yaml,
        )
        defaults["event_log_storage"] = ConfigurableClassData(
            "dagster_cloud.storage.event_logs",
            "GraphQLEventLogStorage",
            empty_yaml,
        )
        defaults["schedule_storage"] = ConfigurableClassData(
            "dagster_cloud.storage.schedules",
            "GraphQLScheduleStorage",
            empty_yaml,
        )

        defaults["compute_logs"] = ConfigurableClassData(
            "dagster_cloud.storage.compute_logs", "CloudComputeLogManager", empty_yaml
        )

        return defaults

    def dispose(self):
        super().dispose()
        self._exit_stack.close()
