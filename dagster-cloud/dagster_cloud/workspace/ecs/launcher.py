from typing import Any, Collection, Dict, List, Optional

import boto3
from dagster import Array, Field, IntSource, Noneable, StringSource, check
from dagster.core.host_representation.grpc_server_registry import GrpcServerEndpoint
from dagster.core.launcher import RunLauncher
from dagster.serdes import ConfigurableClass, ConfigurableClassData
from dagster.utils import merge_dicts
from dagster_aws.ecs import EcsRunLauncher
from dagster_aws.secretsmanager import get_secrets_from_arns, get_tagged_secrets
from dagster_cloud.workspace.origin import CodeDeploymentMetadata

from ..user_code_launcher import ReconcileUserCodeLauncher
from .client import Client
from .service import Service

EcsServerHandleType = Service

DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT = 60


class EcsUserCodeLauncher(ReconcileUserCodeLauncher[EcsServerHandleType], ConfigurableClass):
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
        self.secrets = secrets or []
        self.secrets_tag = secrets_tag

        self._server_process_startup_timeout = check.opt_int_param(
            server_process_startup_timeout,
            "server_process_startup_timeout",
            DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
        )

        self.client = Client(
            cluster_name=self.cluster,
            subnet_ids=self.subnets,
            security_group_ids=security_group_ids,
            service_discovery_namespace_id=self.service_discovery_namespace_id,
            log_group=self.log_group,
            execution_role_arn=self.execution_role_arn,
        )
        super(EcsUserCodeLauncher, self).__init__()

    @property
    def requires_images(self):
        return True

    @classmethod
    def config_type(cls):
        return {
            "cluster": Field(StringSource),
            "subnets": Field(Array(StringSource)),
            "security_group_ids": Field(Array(StringSource), is_required=False),
            "execution_role_arn": Field(StringSource),
            "task_role_arn": Field(StringSource, is_required=False),
            "log_group": Field(StringSource),
            "service_discovery_namespace_id": Field(StringSource),
            "secrets": Field(
                Array(StringSource),
                is_required=False,
                description=(
                    "An array of AWS Secrets Manager secrets arns. These secrets will "
                    "be mounted as environment variabls in the container."
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
            "server_process_startup_timeout": Field(
                IntSource,
                is_required=False,
                default_value=DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
                description="Timeout when waiting for a code server to be ready after its deployment is created",
            ),
        }

    @staticmethod
    def from_config_value(inst_data: ConfigurableClassData, config_value: Dict[str, Any]):
        return EcsUserCodeLauncher(inst_data=inst_data, **config_value)

    @property
    def inst_data(self) -> ConfigurableClassData:
        return self._inst_data

    def _create_new_server_endpoint(
        self, location_name: str, metadata: CodeDeploymentMetadata
    ) -> GrpcServerEndpoint:
        port = 4000
        service = self.client.create_service(
            name=location_name,
            image=metadata.image,
            command=metadata.get_grpc_server_command(),
            env=metadata.get_grpc_server_env(port),
            tags={"dagster/location_name": location_name},
            task_role_arn=self.task_role_arn,
            secrets=merge_dicts(
                (
                    get_tagged_secrets(self.secrets_manager, self.secrets_tag)
                    if self.secrets_tag
                    else {}
                ),
                get_secrets_from_arns(self.secrets_manager, self.secrets),
            ),
        )
        self._logger.info(
            "Created a new service at hostname {} for location {}, waiting for it to be ready...".format(
                service.hostname, location_name
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
        self, location_name: str
    ) -> Collection[EcsServerHandleType]:
        tags = {"dagster/location_name": location_name}
        services = self.client.list_services()
        location_services = [
            service for service in services if tags.items() <= service.tags.items()
        ]
        return location_services

    def _cleanup_servers(self):
        for service in self.client.list_services():
            if "dagster/location_name" in service.tags.keys():
                self._remove_server_handle(service)

    def get_step_handler(self, _execution_config):
        pass

    def run_launcher(self) -> RunLauncher:
        launcher = EcsRunLauncher(secrets=self.secrets, secrets_tag=self.secrets_tag)
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
