import copy
import uuid
from contextlib import ExitStack
from functools import lru_cache
from typing import Any, Dict, List, Optional

import yaml
from dagster import (
    Field,
    _check as check,
)
from dagster._config import process_config
from dagster._core.errors import DagsterInvalidConfigError, DagsterInvariantViolationError
from dagster._core.instance import DagsterInstance
from dagster._core.instance.config import config_field_for_configurable_class
from dagster._core.instance.ref import ConfigurableClassData, InstanceRef, configurable_class_data
from dagster._core.launcher import DefaultRunLauncher, RunLauncher
from dagster._core.storage.pipeline_run import DagsterRun
from dagster_cloud_cli.core.graphql_client import (
    create_cloud_requests_session,
    create_proxy_client,
    get_agent_headers,
)
from dagster_cloud_cli.core.headers.auth import DagsterCloudInstanceScope

from ..auth.constants import get_organization_name_from_agent_token
from ..storage.client import dagster_cloud_api_config
from ..util import get_env_names_from_config, is_isolated_run


class DagsterCloudInstance(DagsterInstance):
    @property
    def telemetry_enabled(self) -> bool:
        return False


class DagsterCloudAgentInstance(DagsterCloudInstance):
    def __init__(
        self, *args, dagster_cloud_api, user_code_launcher=None, agent_replicas=None, **kwargs
    ):
        super().__init__(*args, **kwargs)

        self._unprocessed_dagster_cloud_api_config = dagster_cloud_api
        self._dagster_cloud_api_config = self._get_processed_config(
            "dagster_cloud_api", dagster_cloud_api, dagster_cloud_api_config()
        )

        check.invariant(
            not (
                self._dagster_cloud_api_config.get("deployment")
                and self._dagster_cloud_api_config.get("deployments")
            ),
            "Cannot set both deployment and deployments in `dagster_cloud_api`",
        )

        self._user_code_launcher_data = (
            configurable_class_data(user_code_launcher) if user_code_launcher else None
        )

        if not self._user_code_launcher_data:
            # This is a user facing error. We should have more actionable advice and link to docs here.
            raise DagsterInvariantViolationError(
                "User code launcher is not configured for DagsterCloudAgentInstance. Configure a"
                " user code launcher under the user_code_launcher: key in your dagster.yaml file."
            )

        self._exit_stack = ExitStack()

        self._user_code_launcher = None
        self._requests_session = None
        self._graphql_client = None

        assert self.dagster_cloud_url

        self._agent_replicas_config = self._get_processed_config(
            "agent_replicas", agent_replicas, self._agent_replicas_config_schema()
        )

        self._instance_uuid = str(uuid.uuid4())

    def _get_processed_config(
        self, name: str, config: Optional[Dict[str, Any]], config_type: Dict[str, Any]
    ):
        config_dict = check.opt_dict_param(config, "config", key_type=str)
        processed_config = process_config(config_type, config_dict)
        if not processed_config.success:
            raise DagsterInvalidConfigError(
                f"Errors whilst loading {name} config",
                processed_config.errors,
                config_dict,
            )
        return processed_config.value

    def _dagster_cloud_api_config_for_deployment(self, deployment_name: Optional[str]):
        new_api_config = dict(copy.deepcopy(self._dagster_cloud_api_config))
        if deployment_name:
            new_api_config["deployment"] = deployment_name
            if self.includes_branch_deployments:
                del new_api_config["branch_deployments"]
            if "deployments" in new_api_config:
                del new_api_config["deployments"]
            if "all_serverless_deployments" in new_api_config:
                del new_api_config["all_serverless_deployments"]
        else:
            if "deployment" in new_api_config:
                del new_api_config["deployment"]
            if "deployments" in new_api_config:
                del new_api_config["deployments"]

        return new_api_config

    def ref_for_deployment(self, deployment_name: str):
        my_ref = self.get_ref()
        my_custom_instance_class_data = my_ref.custom_instance_class_data
        new_class_data = _cached_inject_deployment(my_custom_instance_class_data, deployment_name)

        return my_ref._replace(custom_instance_class_data=new_class_data)

    def organization_scoped_graphql_client(self):
        return create_proxy_client(
            self.requests_session,
            self.dagster_cloud_graphql_url,
            self._dagster_cloud_api_config_for_deployment(None),
            scope=DagsterCloudInstanceScope.ORGANIZATION,
        )

    def graphql_client_for_deployment(self, deployment_name: Optional[str]):
        return create_proxy_client(
            self.requests_session,
            self.dagster_cloud_graphql_url,
            self._dagster_cloud_api_config_for_deployment(deployment_name),
            scope=DagsterCloudInstanceScope.DEPLOYMENT,
        )

    def headers_for_deployment(self, deployment_name: str):
        return get_agent_headers(
            self._dagster_cloud_api_config_for_deployment(deployment_name),
            DagsterCloudInstanceScope.DEPLOYMENT,
        )

    def create_graphql_client(
        self, scope: DagsterCloudInstanceScope = DagsterCloudInstanceScope.DEPLOYMENT
    ):
        return create_proxy_client(
            self.requests_session,
            self.dagster_cloud_graphql_url,
            self._dagster_cloud_api_config,
            scope=scope,
        )

    @property
    def requests_session(self):
        if self._requests_session is None:
            self._requests_session = self._exit_stack.enter_context(create_cloud_requests_session())

        return self._requests_session

    @property
    def graphql_client(self):
        if self._graphql_client is None:
            self._graphql_client = self.create_graphql_client(
                scope=DagsterCloudInstanceScope.DEPLOYMENT
            )

        return self._graphql_client

    @property
    def dagster_cloud_url(self):
        if "url" in self._dagster_cloud_api_config:
            return self._dagster_cloud_api_config["url"]

        organization = get_organization_name_from_agent_token(self.dagster_cloud_agent_token)
        if not organization:
            raise DagsterInvariantViolationError(
                "Could not derive Dagster Cloud URL from agent token. Create a new agent token or"
                " set the `url` field under `dagster_cloud_api` in your `dagster.yaml`."
            )

        return f"https://{organization}.agent.dagster.cloud"

    @property
    def organization_name(self) -> Optional[str]:
        return get_organization_name_from_agent_token(self.dagster_cloud_agent_token)

    @property
    def deployment_name(self) -> Optional[str]:
        deployment_names = self.deployment_names
        check.invariant(
            len(deployment_names) <= 1,
            "Cannot call instance.deployment_name if multiple deployments are set",
        )
        if not deployment_names:
            return None
        return deployment_names[0]

    @property
    def deployment_names(self) -> List[str]:
        if self._dagster_cloud_api_config.get("deployment"):
            return [self._dagster_cloud_api_config["deployment"]]

        return self._dagster_cloud_api_config.get("deployments", [])

    @property
    def include_all_serverless_deployments(self) -> bool:
        return self._dagster_cloud_api_config.get("all_serverless_deployments")

    @property
    def dagit_url(self):
        organization = get_organization_name_from_agent_token(self.dagster_cloud_agent_token)
        if not organization:
            raise Exception(
                "Could not derive Dagster Cloud URL from agent token to generate a Dagit URL."
                " Generate a new agent token in the Dagit UI."
            )

        deployment = self._dagster_cloud_api_config.get("deployment")
        return f"https://{organization}.dagster.cloud/" + (f"{deployment}/" if deployment else "")

    @property
    def dagster_cloud_graphql_url(self):
        return f"{self.dagster_cloud_url}/graphql"

    @property
    def dagster_cloud_upload_logs_url(self):
        return f"{self.dagster_cloud_url}/upload_logs"

    @property
    def dagster_cloud_gen_logs_url_url(self):
        return f"{self.dagster_cloud_url}/gen_logs_url"

    @property
    def dagster_cloud_upload_job_snap_url(self):
        return f"{self.dagster_cloud_url}/upload_job_snapshot"

    @property
    def dagster_cloud_upload_workspace_entry_url(self):
        return f"{self.dagster_cloud_url}/upload_workspace_entry"

    @property
    def dagster_cloud_upload_api_response_url(self):
        return f"{self.dagster_cloud_url}/upload_api_response"

    def dagster_cloud_api_headers(self, scope: DagsterCloudInstanceScope):
        return get_agent_headers(self._dagster_cloud_api_config, scope=scope)

    @property
    def dagster_cloud_agent_token(self):
        return self._dagster_cloud_api_config.get("agent_token")

    @property
    def dagster_cloud_api_retries(self) -> int:
        return self._dagster_cloud_api_config["retries"]

    @property
    def dagster_cloud_api_timeout(self) -> int:
        return self._dagster_cloud_api_config["timeout"]

    @property
    def dagster_cloud_api_proxies(self) -> Optional[Dict[str, str]]:
        # Requests library modifies the proxies key so create a copy
        return (
            self._dagster_cloud_api_config.get("proxies").copy()
            if self._dagster_cloud_api_config.get("proxies")
            else {}
        )

    @property
    def dagster_cloud_api_agent_label(self) -> Optional[str]:
        return self._dagster_cloud_api_config.get("agent_label")

    @property
    def includes_branch_deployments(self) -> bool:
        return self._dagster_cloud_api_config.get("branch_deployments", False)

    @property
    def instance_uuid(self) -> str:
        return self._instance_uuid

    @property
    def agent_display_name(self) -> str:
        if self.dagster_cloud_api_agent_label:
            return f"Agent {self.instance_uuid[:8]} ({self.dagster_cloud_api_agent_label})"
        else:
            return f"Agent {self.instance_uuid[:8]}"

    @property
    def dagster_cloud_api_env_vars(self) -> List[str]:
        return get_env_names_from_config(
            dagster_cloud_api_config(), self._unprocessed_dagster_cloud_api_config
        )

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
        """
        The agent has two run launchers, isolated and non-isolated. Use get_run_launcher_for_run to
        get the appropriate one. This method will always return the isolated run launcher, which
        is required for some OSS peices like the k8s executor.
        """
        return self.user_code_launcher.run_launcher()

    def get_run_launcher_for_run(self, run: DagsterRun) -> RunLauncher:
        # If the run is isolated, use the isolated run launcher, which is specific to whatever agent
        # type we're using- ECS, K8s, etc.
        #
        # If the run is not isolated, use the DefaultRunLauncher to send it to the gRPC server.
        if is_isolated_run(run):
            return self.user_code_launcher.run_launcher()
        else:
            launcher = DefaultRunLauncher()
            launcher.register_instance(self)
            return launcher

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
            "agent_replicas": Field(cls._agent_replicas_config_schema(), is_required=False),
        }

    @classmethod
    def _agent_replicas_config_schema(cls):
        return {"enabled": Field(bool, is_required=False, default_value=False)}

    def get_required_daemon_types(self):
        return []

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
        defaults["storage"] = ConfigurableClassData(
            module_name="dagster.core.storage.legacy_storage",
            class_name="CompositeStorage",
            config_yaml=yaml.dump(
                {
                    "run_storage": {
                        "module_name": "dagster_cloud.storage.runs",
                        "class_name": "GraphQLRunStorage",
                        "config_yaml": empty_yaml,
                    },
                    "event_log_storage": {
                        "module_name": "dagster_cloud.storage.event_logs",
                        "class_name": "GraphQLEventLogStorage",
                        "config_yaml": empty_yaml,
                    },
                    "schedule_storage": {
                        "module_name": "dagster_cloud.storage.schedules",
                        "class_name": "GraphQLScheduleStorage",
                        "config_yaml": empty_yaml,
                    },
                },
                default_flow_style=False,
            ),
        )

        defaults["compute_logs"] = ConfigurableClassData(
            "dagster_cloud.storage.compute_logs", "CloudComputeLogManager", empty_yaml
        )

        defaults["secrets"] = ConfigurableClassData(
            "dagster_cloud.secrets", "DagsterCloudSecretsLoader", empty_yaml
        )

        return defaults

    def dispose(self):
        super().dispose()
        self._exit_stack.close()

    @property
    def should_start_background_run_thread(self):
        return self.agent_replicas_enabled

    @property
    def agent_replicas_enabled(self):
        return self._agent_replicas_config.get("enabled", False)

    @property
    def dagster_cloud_run_worker_monitoring_interval_seconds(self):
        return 30


@lru_cache(maxsize=100)  # Scales on order of active branch deployments
def _cached_inject_deployment(
    custom_instance_class_data: ConfigurableClassData,
    deployment_name: str,
) -> ConfigurableClassData:
    # incurs costly yaml parse
    config_dict = custom_instance_class_data.config_dict

    config_dict["dagster_cloud_api"]["deployment"] = deployment_name
    if config_dict["dagster_cloud_api"].get("branch_deployments"):
        del config_dict["dagster_cloud_api"]["branch_deployments"]
    if config_dict["dagster_cloud_api"].get("deployments"):
        del config_dict["dagster_cloud_api"]["deployments"]

    return ConfigurableClassData(
        "dagster_cloud.instance",
        "DagsterCloudAgentInstance",
        # incurs costly yaml dump
        yaml.dump(config_dict),
    )
