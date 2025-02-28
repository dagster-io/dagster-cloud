import copy
import socket
import uuid
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Optional, Union

import yaml
from dagster import (
    Array,
    Field,
    String,
    _check as check,
)
from dagster._config import process_config
from dagster._core.errors import DagsterInvalidConfigError, DagsterInvariantViolationError
from dagster._core.instance import DagsterInstance
from dagster._core.instance.config import config_field_for_configurable_class
from dagster._core.instance.ref import InstanceRef, configurable_class_data
from dagster._core.launcher import DefaultRunLauncher, RunLauncher
from dagster._core.storage.dagster_run import DagsterRun
from dagster._serdes import ConfigurableClassData
from dagster_cloud_cli.core.graphql_client import (
    create_agent_graphql_client,
    create_agent_http_client,
    create_graphql_requests_session,
    get_agent_headers,
)
from dagster_cloud_cli.core.headers.auth import DagsterCloudInstanceScope
from urllib3 import Retry

from dagster_cloud.agent import AgentQueuesConfig
from dagster_cloud.version import __version__

from ..auth.constants import get_organization_name_from_agent_token
from ..opentelemetry.config import opentelemetry_config_schema
from ..opentelemetry.controller import OpenTelemetryController
from ..storage.client import dagster_cloud_api_config
from ..util import get_env_names_from_config, is_isolated_run

if TYPE_CHECKING:
    from requests import Session

    from dagster_cloud.workspace.user_code_launcher.user_code_launcher import (
        DagsterCloudUserCodeLauncher,
    )


class DagsterCloudInstance(DagsterInstance):
    @property
    def telemetry_enabled(self) -> bool:
        return False

    @property
    def run_retries_max_retries(self) -> int:
        raise NotImplementedError(
            "run_retries.max_retries is a deployment setting and can only be accessed by a DeploymentScopedHostInstance"
        )

    @property
    def run_retries_retry_on_asset_or_op_failure(self) -> bool:
        raise NotImplementedError(
            "run_retries.retry_on_asset_or_op_failure is a deployment setting and can only be accessed by a DeploymentScopedHostInstance"
        )


class DagsterCloudAgentInstance(DagsterCloudInstance):
    def __init__(
        self,
        *args,
        dagster_cloud_api,
        user_code_launcher=None,
        agent_replicas=None,
        isolated_agents=None,
        agent_queues=None,
        agent_metrics=None,
        opentelemetry=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self._unprocessed_dagster_cloud_api_config = dagster_cloud_api
        self._dagster_cloud_api_config = self._get_processed_config(
            "dagster_cloud_api", dagster_cloud_api, dagster_cloud_api_config()
        )

        check.invariant(
            not (
                self._dagster_cloud_api_config.get("deployment")  # pyright: ignore[reportOptionalMemberAccess]
                and self._dagster_cloud_api_config.get("deployments")  # pyright: ignore[reportOptionalMemberAccess]
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
        self._graphql_requests_session: Optional[Session] = None
        self._rest_requests_session: Optional[Session] = None
        self._graphql_client = None
        self._http_client = None

        assert self.dagster_cloud_url

        # Handle backcompat between isolated_agents and agent_replicas
        if isolated_agents and agent_replicas:
            raise Exception(
                "Cannot provide both isolated_agents and agent_replicas configuration. Please only"
                " provide one of these."
            )
        if agent_replicas:
            self._isolated_agents = self._get_processed_config(
                "agent_replicas", agent_replicas, self._isolated_agents_config_schema()
            )
        elif isolated_agents:
            self._isolated_agents = self._get_processed_config(
                "isolated_agents", isolated_agents, self._isolated_agents_config_schema()
            )
        else:
            self._isolated_agents = None

        processed_agent_queues_config = self._get_processed_config(
            "agent_queues", agent_queues, self._agent_queues_config_schema()
        )
        self.agent_queues_config = AgentQueuesConfig(**processed_agent_queues_config)  # pyright: ignore[reportCallIssue]

        self._opentelemetry_config: Optional[Mapping[str, Any]] = self._get_processed_config(
            "opentelemetry", opentelemetry, opentelemetry_config_schema()
        )

        self._opentelemetry_controller: Optional[OpenTelemetryController] = None

        self._instance_uuid = str(uuid.uuid4())

    def _get_processed_config(
        self, name: str, config: Optional[dict[str, Any]], config_type: dict[str, Any]
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
        new_api_config = dict(copy.deepcopy(self._dagster_cloud_api_config))  # pyright: ignore[reportArgumentType,reportCallIssue]
        if deployment_name:
            new_api_config["deployment"] = deployment_name  # pyright: ignore[reportArgumentType]
            if self.includes_branch_deployments:
                del new_api_config["branch_deployments"]  # pyright: ignore[reportArgumentType]
            if "deployments" in new_api_config:
                del new_api_config["deployments"]  # pyright: ignore[reportArgumentType]
            if "all_serverless_deployments" in new_api_config:
                del new_api_config["all_serverless_deployments"]  # pyright: ignore[reportArgumentType]
        else:
            if "deployment" in new_api_config:
                del new_api_config["deployment"]  # pyright: ignore[reportArgumentType]
            if "deployments" in new_api_config:
                del new_api_config["deployments"]  # pyright: ignore[reportArgumentType]

        return new_api_config

    def ref_for_deployment(self, deployment_name: str) -> InstanceRef:
        my_ref = self.get_ref()
        my_custom_instance_class_data = my_ref.custom_instance_class_data
        new_class_data = _cached_inject_deployment(my_custom_instance_class_data, deployment_name)

        return my_ref._replace(custom_instance_class_data=new_class_data)

    def organization_scoped_graphql_client(self):
        return create_agent_graphql_client(
            self.client_managed_retries_requests_session,
            self.dagster_cloud_graphql_url,
            self._dagster_cloud_api_config_for_deployment(None),  # pyright: ignore[reportArgumentType]
            scope=DagsterCloudInstanceScope.ORGANIZATION,
        )

    def graphql_client_for_deployment(self, deployment_name: Optional[str]):
        return create_agent_graphql_client(
            self.client_managed_retries_requests_session,
            self.dagster_cloud_graphql_url,
            self._dagster_cloud_api_config_for_deployment(deployment_name),  # pyright: ignore[reportArgumentType]
            scope=DagsterCloudInstanceScope.DEPLOYMENT,
        )

    def headers_for_deployment(self, deployment_name: str):
        return get_agent_headers(
            self._dagster_cloud_api_config_for_deployment(deployment_name),  # pyright: ignore[reportArgumentType]
            DagsterCloudInstanceScope.DEPLOYMENT,
        )

    def create_graphql_client(
        self, scope: DagsterCloudInstanceScope = DagsterCloudInstanceScope.DEPLOYMENT
    ):
        return create_agent_graphql_client(
            self.client_managed_retries_requests_session,
            self.dagster_cloud_graphql_url,
            self._dagster_cloud_api_config,  # pyright: ignore[reportArgumentType]
            scope=scope,
        )

    def _translate_socket_param(self, socket_option: Union[str, int]):
        if isinstance(socket_option, str):
            check.invariant(
                hasattr(socket, socket_option),
                f"socket module does not have an {socket_option} attribute",
            )
            return getattr(socket, socket_option)
        else:
            return socket_option

    def _socket_options(self):
        if self._dagster_cloud_api_config.get("socket_options") is None:  # pyright: ignore[reportOptionalMemberAccess]
            return None

        translated_socket_options = []
        for socket_option in self._dagster_cloud_api_config["socket_options"]:  # pyright: ignore[reportOptionalSubscript]
            check.invariant(
                len(socket_option) == 3, "Each socket option must be a list of three values"
            )
            socket_param_1, socket_param_2, socket_val = socket_option
            translated_socket_options.append(
                (
                    self._translate_socket_param(socket_param_1),
                    self._translate_socket_param(socket_param_2),
                    socket_val,
                )
            )

        return translated_socket_options

    @property
    def client_managed_retries_requests_session(self):
        """A shared requests Session to use between GraphQL clients.

        Retries handled in GraphQL client layer.
        """
        if self._graphql_requests_session is None:
            self._graphql_requests_session = self._exit_stack.enter_context(
                create_graphql_requests_session(
                    adapter_kwargs=dict(socket_options=self._socket_options())
                )
            )

        return self._graphql_requests_session

    @property
    def requests_managed_retries_session(self):
        """A requests session to use for non-GraphQL Rest API requests.

        Retries handled by requests.
        """
        if self._rest_requests_session is None:
            self._rest_requests_session = self._exit_stack.enter_context(
                create_graphql_requests_session(
                    adapter_kwargs=dict(
                        max_retries=Retry(
                            total=self.dagster_cloud_api_retries,
                            backoff_factor=self._dagster_cloud_api_config["backoff_factor"],  # pyright: ignore[reportOptionalSubscript]
                        ),
                        socket_options=self._socket_options(),
                    )
                )
            )
        return self._rest_requests_session

    @property
    def graphql_client(self):
        if self._graphql_client is None:
            self._graphql_client = self.create_graphql_client(
                scope=DagsterCloudInstanceScope.DEPLOYMENT
            )

        return self._graphql_client

    @property
    def http_client(self):
        if self._http_client is None:
            self._http_client = create_agent_http_client(
                self.client_managed_retries_requests_session,
                self._dagster_cloud_api_config,  # pyright: ignore[reportArgumentType]
                scope=DagsterCloudInstanceScope.DEPLOYMENT,
            )

        return self._http_client

    @property
    def dagster_cloud_url(self):
        if "url" in self._dagster_cloud_api_config:  # pyright: ignore[reportOperatorIssue]
            return self._dagster_cloud_api_config["url"]  # pyright: ignore[reportOptionalSubscript]

        organization = get_organization_name_from_agent_token(self.dagster_cloud_agent_token)  # pyright: ignore[reportArgumentType]
        if not organization:
            raise DagsterInvariantViolationError(
                "Could not derive Dagster Cloud URL from agent token. Create a new agent token or"
                " set the `url` field under `dagster_cloud_api` in your `dagster.yaml`."
            )

        return f"https://{organization}.agent.dagster.cloud"

    @property
    def organization_name(self) -> Optional[str]:
        return get_organization_name_from_agent_token(self.dagster_cloud_agent_token)  # pyright: ignore[reportArgumentType]

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
    def deployment_names(self) -> list[str]:
        if self._dagster_cloud_api_config.get("deployment"):  # pyright: ignore[reportOptionalMemberAccess]
            return [self._dagster_cloud_api_config["deployment"]]  # pyright: ignore[reportOptionalSubscript]

        return self._dagster_cloud_api_config.get("deployments", [])  # pyright: ignore[reportOptionalMemberAccess]

    @property
    def include_all_serverless_deployments(self) -> bool:
        return self._dagster_cloud_api_config.get("all_serverless_deployments")  # pyright: ignore[reportOptionalMemberAccess,reportReturnType]

    @property
    def dagit_url(self):
        organization = get_organization_name_from_agent_token(self.dagster_cloud_agent_token)  # pyright: ignore[reportArgumentType]
        if not organization:
            raise Exception(
                "Could not derive Dagster Cloud URL from agent token to generate a Dagit URL."
                " Generate a new agent token in the Dagit UI."
            )

        deployment = self._dagster_cloud_api_config.get("deployment")  # pyright: ignore[reportOptionalMemberAccess]
        return f"https://{organization}.dagster.cloud/" + (f"{deployment}/" if deployment else "")

    @property
    def dagster_cloud_graphql_url(self):
        return f"{self.dagster_cloud_url}/graphql"

    @property
    def dagster_cloud_store_events_url(self):
        return f"{self.dagster_cloud_url}/store_events"

    @property
    def dagster_cloud_upload_logs_url(self):
        return f"{self.dagster_cloud_url}/upload_logs"

    @property
    def dagster_cloud_gen_logs_url_url(self):
        return f"{self.dagster_cloud_url}/gen_logs_url"

    @property
    def dagster_cloud_gen_insights_url_url(self) -> str:
        return f"{self.dagster_cloud_url}/gen_insights_url"

    @property
    def dagster_cloud_upload_job_snap_url(self):
        return f"{self.dagster_cloud_url}/upload_job_snapshot"

    @property
    def dagster_cloud_upload_workspace_entry_url(self):
        return f"{self.dagster_cloud_url}/upload_workspace_entry"

    @property
    def dagster_cloud_upload_api_response_url(self):
        return f"{self.dagster_cloud_url}/upload_api_response"

    @property
    def dagster_cloud_check_snapshot_url(self):
        return f"{self.dagster_cloud_url}/check_snapshot"

    @property
    def dagster_cloud_confirm_upload_url(self):
        return f"{self.dagster_cloud_url}/confirm_upload"

    @property
    def dagster_cloud_code_location_update_result_url(self):
        return f"{self.dagster_cloud_url}/code_location_update_result"

    def dagster_cloud_api_headers(self, scope: DagsterCloudInstanceScope):
        return get_agent_headers(self._dagster_cloud_api_config, scope=scope)  # pyright: ignore[reportArgumentType]

    @property
    def dagster_cloud_agent_token(self):
        return self._dagster_cloud_api_config.get("agent_token")  # pyright: ignore[reportOptionalMemberAccess]

    @property
    def dagster_cloud_api_retries(self) -> int:
        return self._dagster_cloud_api_config["retries"]  # pyright: ignore[reportOptionalSubscript]

    @property
    def dagster_cloud_api_timeout(self) -> int:
        return self._dagster_cloud_api_config["timeout"]  # pyright: ignore[reportOptionalSubscript]

    @property
    def dagster_cloud_api_proxies(self) -> Optional[dict[str, str]]:
        # Requests library modifies the proxies key so create a copy
        return (
            self._dagster_cloud_api_config.get("proxies").copy()  # pyright: ignore[reportOptionalMemberAccess]
            if self._dagster_cloud_api_config.get("proxies")  # pyright: ignore[reportOptionalMemberAccess]
            else {}
        )

    @property
    def dagster_cloud_api_agent_label(self) -> Optional[str]:
        return self._dagster_cloud_api_config.get("agent_label")  # pyright: ignore[reportOptionalMemberAccess]

    @property
    def includes_branch_deployments(self) -> bool:
        return self._dagster_cloud_api_config.get("branch_deployments", False)  # pyright: ignore[reportOptionalMemberAccess]

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
    def dagster_cloud_api_env_vars(self) -> list[str]:
        return get_env_names_from_config(
            dagster_cloud_api_config(), self._unprocessed_dagster_cloud_api_config
        )

    @property
    def user_code_launcher(self) -> "DagsterCloudUserCodeLauncher":
        # Lazily load in case the user code launcher requires dependencies (like dagster-k8s)
        # that we don't neccesarily need to load in every context that loads a
        # DagsterCloudAgentInstance (for example, a step worker)
        from dagster_cloud.workspace.user_code_launcher.user_code_launcher import (
            DagsterCloudUserCodeLauncher,
        )

        if not self._user_code_launcher:
            self._user_code_launcher = self._exit_stack.enter_context(
                self._user_code_launcher_data.rehydrate(as_type=DagsterCloudUserCodeLauncher)  # type: ignore  # (possible none)
            )
            self._user_code_launcher.register_instance(self)
        return self._user_code_launcher

    @property
    def run_launcher(self) -> RunLauncher:
        """The agent has two run launchers, isolated and non-isolated. Use get_run_launcher_for_run to
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
    def get():
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
            "isolated_agents": Field(cls._isolated_agents_config_schema(), is_required=False),
            "agent_replicas": Field(
                cls._isolated_agents_config_schema(), is_required=False
            ),  # deprecated in favor of isolated_agents
            "agent_queues": Field(cls._agent_queues_config_schema(), is_required=False),
            "opentelemetry": Field(
                opentelemetry_config_schema(), is_required=False, default_value={"enabled": False}
            ),
        }

    @classmethod
    def _code_server_metrics_config_schema(cls):
        return {"enabled": Field(bool, is_required=False, default_value=False)}

    @classmethod
    def _isolated_agents_config_schema(cls):
        return {"enabled": Field(bool, is_required=False, default_value=False)}

    @classmethod
    def _agent_queues_config_schema(cls):
        return {
            "include_default_queue": Field(bool, default_value=True),
            "additional_queues": Field(Array(String), is_required=False),
        }

    def get_required_daemon_types(self) -> Sequence[str]:
        return []

    @staticmethod
    def config_defaults(base_dir):
        defaults = InstanceRef.config_defaults(base_dir)

        empty_yaml = yaml.dump({})

        defaults["run_storage"] = ConfigurableClassData(  # pyright: ignore[reportIndexIssue]
            "dagster_cloud.storage.runs",
            "GraphQLRunStorage",
            empty_yaml,
        )
        defaults["event_log_storage"] = ConfigurableClassData(  # pyright: ignore[reportIndexIssue]
            "dagster_cloud.storage.event_logs",
            "GraphQLEventLogStorage",
            empty_yaml,
        )
        defaults["schedule_storage"] = ConfigurableClassData(  # pyright: ignore[reportIndexIssue]
            "dagster_cloud.storage.schedules",
            "GraphQLScheduleStorage",
            empty_yaml,
        )
        defaults["storage"] = ConfigurableClassData(  # pyright: ignore[reportIndexIssue]
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

        defaults["compute_logs"] = ConfigurableClassData(  # pyright: ignore[reportIndexIssue]
            "dagster_cloud.storage.compute_logs", "CloudComputeLogManager", empty_yaml
        )

        defaults["secrets"] = ConfigurableClassData(  # pyright: ignore[reportIndexIssue]
            "dagster_cloud.secrets", "DagsterCloudSecretsLoader", empty_yaml
        )

        return defaults

    def dispose(self) -> None:
        super().dispose()
        if self._opentelemetry_controller:
            self._opentelemetry_controller.dispose()
            self._opentelemetry_controller = None
        self._exit_stack.close()

    @property
    def should_start_background_run_thread(self) -> bool:
        # If using isolated agents (that is, agents cannot see each other's grpc
        # servers), TERMINATE_RUN requests can't be served by the Dagster Cloud API,
        # which will send requests to any agent. Only the agent with the run can
        # terminate it - so we need to start a background run thread to monitor for
        # runs moved into a canceling state.
        return self.is_using_isolated_agents

    @property
    def is_using_isolated_agents(self) -> bool:
        return self._isolated_agents is not None and self._isolated_agents.get("enabled", False)

    @property
    def dagster_cloud_run_worker_monitoring_interval_seconds(self) -> int:
        # potentially overridden interval in the serverless user code launcher
        return 30

    @property
    def opentelemetry(self) -> OpenTelemetryController:
        if not self._opentelemetry_controller:
            self._opentelemetry_controller = OpenTelemetryController(
                instance_id=self.instance_uuid,
                version=__version__,
                config=self._opentelemetry_config,
            )

        return self._opentelemetry_controller


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
