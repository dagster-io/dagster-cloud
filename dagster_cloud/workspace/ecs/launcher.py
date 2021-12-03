from typing import Any, Dict, Iterable, List, Optional

import boto3
from dagster import Array, Field, StringSource, check
from dagster.core.host_representation.grpc_server_registry import GrpcServerEndpoint
from dagster.core.launcher import RunLauncher
from dagster.serdes import ConfigurableClass, ConfigurableClassData
from dagster_aws.ecs import EcsRunLauncher
from dagster_cloud.workspace.origin import CodeDeploymentMetadata

from ..user_code_launcher import ReconcileUserCodeLauncher
from .client import Client
from .service import Service

EcsServerHandleType = Service


class EcsUserCodeLauncher(ReconcileUserCodeLauncher[EcsServerHandleType], ConfigurableClass):
    def __init__(
        self,
        cluster: str,
        subnets: List[str],
        execution_role_arn: str,
        log_group: str,
        service_discovery_namespace_id: str,
        task_role_arn: str = None,
        inst_data: Optional[ConfigurableClassData] = None,
    ):
        self.ecs = boto3.client("ecs")
        self.logs = boto3.client("logs")
        self.service_discovery = boto3.client("servicediscovery")

        # TODO: Default to default cluster
        self.cluster = cluster
        # TODO: Default to default networking
        self.subnets = subnets
        self.service_discovery_namespace_id = service_discovery_namespace_id
        self.execution_role_arn = execution_role_arn
        self.task_role_arn = task_role_arn
        # TODO: Create a log group if one doesn't exist?
        self.log_group = log_group
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

        self.client = Client(
            cluster_name=self.cluster,
            subnet_ids=self.subnets,
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
            "execution_role_arn": Field(StringSource),
            "task_role_arn": Field(StringSource, is_required=False),
            "log_group": Field(StringSource),
            "service_discovery_namespace_id": Field(StringSource),
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

        command = [
            "dagster",
            "api",
            "grpc",
            "-p",
            str(port),
            "-h",
            "0.0.0.0",
            "--lazy-load-user-code",
        ]

        env = {"DAGSTER_CURRENT_IMAGE": metadata.image}
        if metadata.python_file:
            env["DAGSTER_CLI_API_GRPC_PYTHON_FILE"] = metadata.python_file
        else:
            env["DAGSTER_CLI_API_GRPC_PACKAGE_NAME"] = metadata.package_name

        service = self.client.create_service(
            name=location_name,
            image=metadata.image,
            command=command,
            env=env,
            tags={"dagster/location_name": location_name},
            task_role_arn=self.task_role_arn,
        )
        server_id = self._wait_for_server(host=service.hostname, port=4000, timeout=60)

        endpoint = GrpcServerEndpoint(
            server_id=server_id,
            host=service.hostname,
            port=port,
            socket=None,
        )
        return endpoint

    def _remove_server_handle(self, server_handle: EcsServerHandleType) -> None:
        self.client.delete_service(server_handle)

    def _get_server_handles_for_location(self, location_name: str) -> Iterable[EcsServerHandleType]:
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
        launcher = EcsRunLauncher()
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
