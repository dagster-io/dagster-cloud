from typing import Any, Collection, Dict, List, Optional

import boto3
from dagster import Array, Field, IntSource, Noneable, ScalarUnion, StringSource
from dagster import _check as check
from dagster._core.host_representation.grpc_server_registry import GrpcServerEndpoint
from dagster._core.launcher import RunLauncher
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils import merge_dicts
from dagster_aws.ecs import EcsRunLauncher
from dagster_aws.ecs.container_context import EcsContainerContext
from dagster_aws.secretsmanager import get_secrets_from_arns
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata

from ..user_code_launcher import (
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
    SHARED_USER_CODE_LAUNCHER_CONFIG,
    DagsterCloudUserCodeLauncher,
)
from ..user_code_launcher.utils import deterministic_label_for_location
from .client import DEFAULT_ECS_GRACE_PERIOD, DEFAULT_ECS_TIMEOUT, Client
from .service import Service
from .utils import get_ecs_human_readable_label, unique_ecs_resource_name

EcsServerHandleType = Service


class EcsUserCodeLauncher(DagsterCloudUserCodeLauncher[EcsServerHandleType], ConfigurableClass):
    def __init__(
        self,
        cluster: str,
        subnets: List[str],
        execution_role_arn: str,
        log_group: str,
        service_discovery_namespace_id: str,
        task_role_arn: str = None,
        security_group_ids: List[str] = None,
        server_process_startup_timeout=None,
        inst_data: Optional[ConfigurableClassData] = None,
        secrets=None,
        secrets_tag=None,
        env_vars=None,
        server_ttl=None,
        ecs_timeout=None,
        ecs_grace_period=None,
    ):
        self.ecs = boto3.client("ecs")
        self.logs = boto3.client("logs")
        self.service_discovery = boto3.client("servicediscovery")
        self.secrets_manager = boto3.client("secretsmanager")

        self.cluster = cluster
        self.subnets = subnets
        self.security_group_ids = security_group_ids
        self.service_discovery_namespace_id = service_discovery_namespace_id
        self.execution_role_arn = execution_role_arn
        self.task_role_arn = task_role_arn
        self.log_group = log_group
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self.secrets = check.opt_list_param(secrets, "secrets")
        self.env_vars = check.opt_list_param(env_vars, "env_vars")

        if all(isinstance(secret, str) for secret in self.secrets):
            self.secrets = [
                {"name": name, "valueFrom": value_from}
                for name, value_from in get_secrets_from_arns(
                    self.secrets_manager, self.secrets
                ).items()
            ]

        self.secrets_tag = secrets_tag

        self._server_process_startup_timeout = check.opt_int_param(
            server_process_startup_timeout,
            "server_process_startup_timeout",
            DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
        )

        self._ecs_timeout = check.opt_int_param(
            ecs_timeout,
            "ecs_timeout",
            DEFAULT_ECS_TIMEOUT,
        )

        self._ecs_grace_period = check.opt_int_param(
            ecs_grace_period,
            "ecs_grace_period",
            DEFAULT_ECS_GRACE_PERIOD,
        )

        self.client = Client(
            cluster_name=self.cluster,
            subnet_ids=self.subnets,
            security_group_ids=security_group_ids,
            service_discovery_namespace_id=self.service_discovery_namespace_id,
            log_group=self.log_group,
            execution_role_arn=self.execution_role_arn,
            timeout=self._ecs_timeout,
            grace_period=self._ecs_grace_period,
        )
        super(EcsUserCodeLauncher, self).__init__(server_ttl=server_ttl)

    @property
    def requires_images(self):
        return True

    @classmethod
    def config_type(cls):
        return merge_dicts(
            {
                "cluster": Field(StringSource),
                "subnets": Field(Array(StringSource)),
                "security_group_ids": Field(Array(StringSource), is_required=False),
                "execution_role_arn": Field(StringSource),
                "task_role_arn": Field(StringSource, is_required=False),
                "log_group": Field(StringSource),
                "service_discovery_namespace_id": Field(StringSource),
                "secrets": Field(
                    Array(
                        ScalarUnion(
                            scalar_type=str,
                            non_scalar_schema={"name": StringSource, "valueFrom": StringSource},
                        )
                    ),
                    is_required=False,
                    description=(
                        "An array of AWS Secrets Manager secrets. These secrets will "
                        "be mounted as environment variabls in the container. See "
                        "https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_Secret.html."
                    ),
                ),
                "secrets_tag": Field(
                    Noneable(StringSource),
                    is_required=False,
                    description=(
                        "AWS Secrets Manager secrets with this tag will be mounted as "
                        "environment variables in the container."
                    ),
                ),
                "env_vars": Field(
                    [StringSource],
                    is_required=False,
                    description="List of environment variable names to include in the ECS task. "
                    "Each can be of the form KEY=VALUE or just KEY (in which case the value will be pulled "
                    "from the current process)",
                ),
                "server_process_startup_timeout": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
                    description="Timeout when waiting for a code server to be ready after it is created",
                ),
                "ecs_timeout": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_ECS_TIMEOUT,
                    description="How long (in seconds) to poll against ECS API endpoints",
                ),
                "ecs_grace_period": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_ECS_GRACE_PERIOD,
                    description="How long (in seconds) to continue polling if an ECS API endpoint fails "
                    "(because the ECS API is eventually consistent)",
                ),
            },
            SHARED_USER_CODE_LAUNCHER_CONFIG,
        )

    @staticmethod
    def from_config_value(inst_data: ConfigurableClassData, config_value: Dict[str, Any]):
        return EcsUserCodeLauncher(inst_data=inst_data, **config_value)

    @property
    def inst_data(self) -> Optional[ConfigurableClassData]:
        return self._inst_data

    def _create_new_server_endpoint(
        self, deployment_name: str, location_name: str, metadata: CodeDeploymentMetadata
    ) -> GrpcServerEndpoint:
        return self._launch(
            deployment_name,
            location_name,
            metadata,
            command=metadata.get_grpc_server_command(),
        )

    def _launch(
        self,
        deployment_name: str,
        location_name: str,
        metadata: CodeDeploymentMetadata,
        command: List[str],
        additional_environment: Dict[str, Any] = None,
    ) -> GrpcServerEndpoint:
        port = 4000

        container_context = EcsContainerContext(
            secrets=self.secrets,
            secrets_tags=[self.secrets_tag] if self.secrets_tag else [],
            env_vars=self.env_vars
            + [f"{k}={v}" for k, v in (metadata.cloud_context_env or {}).items()],
        ).merge(EcsContainerContext.create_from_config(metadata.container_context))

        environment = merge_dicts(
            container_context.get_environment_dict(),
            (additional_environment or {}),
            metadata.get_grpc_server_env(port),
        )

        service = self.client.create_service(
            name=unique_ecs_resource_name(deployment_name, location_name),
            image=metadata.image,
            command=command,
            env=environment,
            tags={
                "dagster/deployment_name": get_ecs_human_readable_label(deployment_name),
                "dagster/location_name": get_ecs_human_readable_label(
                    location_name,
                ),
                "dagster/location_hash": deterministic_label_for_location(
                    deployment_name, location_name
                ),
            },
            task_role_arn=self.task_role_arn,
            secrets=container_context.get_secrets_dict(self.secrets_manager),
            append_unique_suffix=False,  # already covered by unique_resource_name
        )
        self._logger.info(
            "Created a new service at hostname {} for {}:{}, waiting for it to be ready...".format(
                service.hostname, deployment_name, location_name
            )
        )
        server_id = self._wait_for_server(
            host=service.hostname, port=port, timeout=self._server_process_startup_timeout
        )

        endpoint = GrpcServerEndpoint(
            server_id=server_id,
            host=service.hostname,
            port=port,
            socket=None,
        )
        return endpoint

    def _remove_server_handle(self, server_handle: EcsServerHandleType) -> None:
        self._logger.info(
            "Deleting service {} at hostname {}...".format(
                server_handle.name, server_handle.hostname
            )
        )
        self.client.delete_service(server_handle)

    def _get_server_handles_for_location(
        self, deployment_name, location_name: str
    ) -> Collection[EcsServerHandleType]:
        tags = {
            "dagster/location_hash": deterministic_label_for_location(
                deployment_name, location_name
            )
        }
        services = self.client.list_services()
        location_services = [
            service for service in services if tags.items() <= service.tags.items()
        ]
        return location_services

    def _cleanup_servers(self):
        for service in self.client.list_services():
            if "dagster/location_name" in service.tags.keys():
                self._remove_server_handle(service)

    def run_launcher(self) -> RunLauncher:
        launcher = EcsRunLauncher(
            secrets=self.secrets, secrets_tag=self.secrets_tag, env_vars=self.env_vars
        )
        launcher.register_instance(self._instance)

        return launcher

    def _get_logs(self, task_arn: str) -> List[str]:
        task_id = task_arn.split("/")[-1]

        task = self.ecs.describe_tasks(cluster=self.cluster, tasks=[task_arn],).get(
            "tasks"
        )[0]
        task_definition = self.ecs.describe_task_definition(
            taskDefinition=task.get("taskDefinitionArn"),
        ).get("taskDefinition")
        container = task_definition.get("containerDefinitions")[0]
        container_name = container.get("name")
        log_stream_prefix = (
            container.get("logConfiguration", {}).get("options", {}).get("awslogs-stream-prefix")
        )

        log_stream = f"{log_stream_prefix}/{container_name}/{task_id}"

        events = self.logs.get_log_events(
            logGroupName=self.log_group,
            logStreamName=log_stream,
        ).get("events")

        return [event.get("message") for event in events]
