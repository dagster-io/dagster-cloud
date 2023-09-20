from pathlib import Path
from typing import Any, Collection, Dict, List, Mapping, Optional, Sequence

import boto3
from dagster import (
    Array,
    Enum,
    EnumValue,
    Field,
    IntSource,
    Noneable,
    ScalarUnion,
    StringSource,
    _check as check,
)
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils.merger import merge_dicts
from dagster_aws.ecs.container_context import EcsContainerContext
from dagster_aws.secretsmanager import get_secrets_from_arns
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata

from dagster_cloud.workspace.config_schema import SHARED_ECS_CONFIG
from dagster_cloud.workspace.ecs.client import DEFAULT_ECS_GRACE_PERIOD, DEFAULT_ECS_TIMEOUT, Client
from dagster_cloud.workspace.ecs.service import Service
from dagster_cloud.workspace.ecs.utils import get_ecs_human_readable_label, unique_ecs_resource_name
from dagster_cloud.workspace.user_code_launcher import (
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
    SHARED_USER_CODE_LAUNCHER_CONFIG,
    DagsterCloudGrpcServer,
    DagsterCloudUserCodeLauncher,
    ServerEndpoint,
)
from dagster_cloud.workspace.user_code_launcher.utils import deterministic_label_for_location

from .client import get_debug_ecs_prompt
from .run_launcher import CloudEcsRunLauncher
from .utils import get_server_task_definition_family

EcsServerHandleType = Service

CONTAINER_NAME = "dagster"
PORT = 4000


class EcsUserCodeLauncher(DagsterCloudUserCodeLauncher[EcsServerHandleType], ConfigurableClass):
    def __init__(
        self,
        cluster: str,
        subnets: List[str],
        execution_role_arn: str,
        log_group: str,
        service_discovery_namespace_id: str,
        task_role_arn: Optional[str] = None,
        security_group_ids: Optional[List[str]] = None,
        inst_data: Optional[ConfigurableClassData] = None,
        secrets=None,
        secrets_tag=None,
        env_vars=None,
        ecs_timeout=None,
        ecs_grace_period=None,
        launch_type: Optional[str] = None,
        server_resources: Optional[Mapping[str, Any]] = None,
        run_resources: Optional[Mapping[str, Any]] = None,
        runtime_platform: Optional[Mapping[str, Any]] = None,
        mount_points: Optional[Sequence[Mapping[str, Any]]] = None,
        volumes: Optional[Sequence[Mapping[str, Any]]] = None,
        server_sidecar_containers: Optional[Sequence[Mapping[str, Any]]] = None,
        run_sidecar_containers: Optional[Sequence[Mapping[str, Any]]] = None,
        server_ecs_tags: Optional[Sequence[Mapping[str, Optional[str]]]] = None,
        run_ecs_tags: Optional[Sequence[Mapping[str, Optional[str]]]] = None,
        **kwargs,
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

        self.launch_type = check.opt_str_param(launch_type, "launch_type", default="FARGATE")

        self.server_resources = check.opt_mapping_param(server_resources, "server_resources")
        self.run_resources = check.opt_mapping_param(run_resources, "run_resources")

        self.runtime_platform = check.opt_mapping_param(runtime_platform, "runtime_platform")

        self.mount_points = check.opt_sequence_param(mount_points, "mount_points")
        self.volumes = check.opt_sequence_param(volumes, "volumes")

        self.server_sidecar_containers = check.opt_sequence_param(
            server_sidecar_containers, "server_sidecar_containers"
        )
        self.run_sidecar_containers = check.opt_sequence_param(
            run_sidecar_containers, "run_sidecar_containers"
        )

        self.server_ecs_tags = check.opt_sequence_param(server_ecs_tags, "server_ecs_tags")
        self.run_ecs_tags = check.opt_sequence_param(run_ecs_tags, "run_ecs_tags")

        self.client = Client(
            cluster_name=self.cluster,
            subnet_ids=self.subnets,
            security_group_ids=security_group_ids,
            service_discovery_namespace_id=self.service_discovery_namespace_id,
            log_group=self.log_group,
            show_debug_cluster_info=self.show_debug_cluster_info,
            timeout=self._ecs_timeout,
            grace_period=self._ecs_grace_period,
            launch_type=self.launch_type,
        )
        super(EcsUserCodeLauncher, self).__init__(**kwargs)

    @property
    def show_debug_cluster_info(self) -> bool:
        return True

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
                    description=(
                        "List of environment variable names to include in the ECS task. Each can be"
                        " of the form KEY=VALUE or just KEY (in which case the value will be pulled"
                        " from the current process)"
                    ),
                ),
                "server_process_startup_timeout": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
                    description=(
                        "Timeout when waiting for a code server to be ready after it is created."
                        " You might want to increase this if your ECS tasks are successfully"
                        " starting but your gRPC server is timing out."
                    ),
                ),
                "ecs_timeout": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_ECS_TIMEOUT,
                    description=(
                        "How long (in seconds) to poll against ECS API endpoints. You might want to"
                        " increase this if your ECS tasks are taking too long to start up."
                    ),
                ),
                "ecs_grace_period": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_ECS_GRACE_PERIOD,
                    description=(
                        "How long (in seconds) to continue polling if an ECS API endpoint fails "
                        "(because the ECS API is eventually consistent)"
                    ),
                ),
                "launch_type": Field(
                    Enum(
                        "EcsLaunchType",
                        [
                            EnumValue("FARGATE"),
                            EnumValue("EC2"),
                        ],
                    ),
                    is_required=False,
                    default_value="FARGATE",
                    description=(
                        "What type of ECS infrastructure to launch the run task in. "
                        "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/launch_types.html"
                    ),
                ),
            },
            SHARED_ECS_CONFIG,
            SHARED_USER_CODE_LAUNCHER_CONFIG,
        )

    @classmethod
    def from_config_value(cls, inst_data: ConfigurableClassData, config_value: Dict[str, Any]):
        return EcsUserCodeLauncher(inst_data=inst_data, **config_value)

    @property
    def inst_data(self) -> Optional[ConfigurableClassData]:
        return self._inst_data

    def _write_liveness_sentinel(self) -> None:
        # Write to a sentinel file to indicate that we've finished our initial
        # reconciliation - this is used to indicate that we're ready to
        # serve requests
        Path("/opt/finished_initial_reconciliation_sentinel.txt").touch(exist_ok=True)
        self._logger.info(
            "Wrote liveness sentinel: indicating that agent is ready to serve requests"
        )

    def _get_grpc_server_sidecars(
        self, container_context: EcsContainerContext
    ) -> Optional[Sequence[Mapping[str, Any]]]:
        return container_context.server_sidecar_containers

    def _get_service_cpu_override(self, container_context: EcsContainerContext) -> Optional[str]:
        return container_context.server_resources.get("cpu")

    def _get_service_memory_override(self, container_context: EcsContainerContext) -> Optional[str]:
        return container_context.server_resources.get("memory")

    def _get_service_ephemeral_storage_override(
        self, container_context: EcsContainerContext
    ) -> Optional[int]:
        return container_context.server_resources.get("ephemeral_storage")

    def _get_service_repository_credentials_override(
        self, container_context: EcsContainerContext
    ) -> Optional[str]:
        return container_context.repository_credentials

    def _get_enable_ecs_exec(self) -> bool:
        return False

    def _get_additional_grpc_server_env(self) -> Dict[str, str]:
        return {}

    def _get_dagster_tags(self, deployment_name: str, location_name: str) -> Dict[str, str]:
        return {
            "dagster/deployment_name": get_ecs_human_readable_label(deployment_name),
            "dagster/location_name": get_ecs_human_readable_label(
                location_name,
            ),
            "dagster/location_hash": deterministic_label_for_location(
                deployment_name, location_name
            ),
        }

    def _start_new_server_spinup(
        self, deployment_name: str, location_name: str, metadata: CodeDeploymentMetadata
    ) -> DagsterCloudGrpcServer:
        if metadata.pex_metadata:
            command = metadata.get_multipex_server_command(PORT)
            additional_env = metadata.get_multipex_server_env()
            tags = {
                "dagster/multipex_server": "1",
                "dagster/agent_id": self._instance.instance_uuid,
            }
        else:
            command = metadata.get_grpc_server_command()
            additional_env = metadata.get_grpc_server_env(
                PORT, location_name, self._instance.ref_for_deployment(deployment_name)
            )
            tags = {"dagster/grpc_server": "1", "dagster/agent_id": self._instance.instance_uuid}

        container_context = EcsContainerContext(
            secrets=self.secrets,
            secrets_tags=[self.secrets_tag] if self.secrets_tag else [],
            env_vars=self.env_vars
            + [f"{k}={v}" for k, v in (metadata.cloud_context_env or {}).items()],
            server_resources=self.server_resources,
            run_resources=self.run_resources,
            task_role_arn=self.task_role_arn,
            execution_role_arn=self.execution_role_arn,
            runtime_platform=self.runtime_platform,
            mount_points=self.mount_points,
            volumes=self.volumes,
            server_sidecar_containers=self.server_sidecar_containers,
            run_sidecar_containers=self.run_sidecar_containers,
            server_ecs_tags=self.server_ecs_tags,
            run_ecs_tags=self.run_ecs_tags,
        ).merge(EcsContainerContext.create_from_config(metadata.container_context))

        environment = merge_dicts(
            container_context.get_environment_dict(),
            additional_env,
            self._get_additional_grpc_server_env(),
        )

        self._logger.info(f"Creating a new service for {deployment_name}:{location_name}...")

        family = get_server_task_definition_family(
            self._instance.organization_name, deployment_name, location_name
        )

        system_tags = {**self._get_dagster_tags(deployment_name, location_name), **tags}
        system_tag_keys = set(system_tags)

        invalid_user_keys = [
            tag["key"] for tag in container_context.server_ecs_tags if tag["key"] in system_tag_keys
        ]
        if invalid_user_keys:
            raise Exception(f"Cannot override system ECS tags: {', '.join(invalid_user_keys)}")

        service = self.client.create_service(
            name=unique_ecs_resource_name(deployment_name, location_name),
            family=family,
            image=metadata.image,
            container_name=CONTAINER_NAME,
            command=command,
            execution_role_arn=container_context.execution_role_arn,
            env=environment,
            tags={
                **self._get_dagster_tags(deployment_name, location_name),
                **tags,
                **{tag["key"]: tag.get("value") for tag in container_context.server_ecs_tags},
            },
            task_role_arn=container_context.task_role_arn,
            secrets=container_context.get_secrets_dict(self.secrets_manager),
            sidecars=self._get_grpc_server_sidecars(container_context),
            logger=self._logger,
            cpu=self._get_service_cpu_override(container_context),
            memory=self._get_service_memory_override(container_context),
            ephemeral_storage=self._get_service_ephemeral_storage_override(container_context),
            repository_credentials=self._get_service_repository_credentials_override(
                container_context
            ),
            allow_ecs_exec=self._get_enable_ecs_exec(),
            runtime_platform=container_context.runtime_platform,
            mount_points=container_context.mount_points,
            volumes=container_context.volumes,
        )
        self._logger.info(
            "Created a new service at hostname {} for {}:{}, waiting for server to be ready..."
            .format(service.hostname, deployment_name, location_name)
        )

        endpoint = ServerEndpoint(
            host=service.hostname,
            port=PORT,
            socket=None,
        )

        return DagsterCloudGrpcServer(service, endpoint, metadata)

    def _check_running_multipex_server(self, multipex_server: DagsterCloudGrpcServer):
        self._logger.info(
            f"Checking whether service {multipex_server.server_handle.name} is ready for existing"
            " multipex server..."
        )
        self.client.check_service_has_running_task(
            multipex_server.server_handle.name, container_name=CONTAINER_NAME, logger=self._logger
        )
        super()._check_running_multipex_server(multipex_server)

    def _wait_for_new_multipex_server(
        self,
        _deployment_name: str,
        _location_name: str,
        server_handle: Service,
        multipex_endpoint: ServerEndpoint,
    ):
        self._logger.info(
            f"Waiting for service {server_handle.name} to be ready for multipex server..."
        )
        task_arn = self.client.wait_for_new_service(
            server_handle, container_name=CONTAINER_NAME, logger=self._logger
        )
        self._wait_for_server_process(
            multipex_endpoint.create_multipex_client(),
            timeout=self._server_process_startup_timeout,
            additional_check=lambda: self.client.assert_task_not_stopped(
                task_arn, CONTAINER_NAME, self._logger
            ),
        )

    def _get_timeout_debug_info(
        self,
        task_arn,
    ):
        sections = []

        try:
            logs = self.client.get_task_logs(task_arn, container_name=CONTAINER_NAME)
            task_logs = "Task logs:\n" + "\n".join(logs) if logs else "No logs in task."
            sections.append(task_logs)
        except:
            self._logger.exception("Error trying to get logs for failed task", task_arn=task_arn)

        if self.show_debug_cluster_info:
            sections.append(get_debug_ecs_prompt(self.cluster, task_arn))

        return "\n\n".join(sections)

    def _wait_for_new_server_ready(
        self,
        _deployment_name: str,
        _location_name: str,
        _metadata: CodeDeploymentMetadata,
        server_handle: Service,
        server_endpoint: ServerEndpoint,
    ) -> None:
        self._logger.info(
            f"Waiting for service {server_handle.name} to be ready for gRPC server..."
        )
        task_arn = self.client.wait_for_new_service(
            server_handle, container_name=CONTAINER_NAME, logger=self._logger
        )
        self._wait_for_dagster_server_process(
            client=server_endpoint.create_client(),
            timeout=self._server_process_startup_timeout,
            additional_check=lambda: self.client.assert_task_not_stopped(
                task_arn, CONTAINER_NAME, self._logger
            ),
            get_timeout_debug_info=lambda: self._get_timeout_debug_info(task_arn),
        )

    def _remove_server_handle(self, server_handle: EcsServerHandleType) -> None:
        self._logger.info(
            f"Deleting service {server_handle.name} at hostname {server_handle.hostname}..."
        )
        self.client.delete_service(server_handle)
        self._logger.info(
            f"Deleted service {server_handle.name} at hostname {server_handle.hostname}."
        )

    def _get_multipex_server_handles_for_location(
        self, deployment_name, location_name: str
    ) -> Collection[EcsServerHandleType]:
        tags = {
            "dagster/location_hash": deterministic_label_for_location(
                deployment_name, location_name
            ),
            "dagster/multipex_server": "1",
            "dagster/agent_id": self._instance.instance_uuid,
        }
        return self.client.list_services(tags)

    def _get_standalone_dagster_server_handles_for_location(
        self, deployment_name, location_name: str
    ) -> Collection[EcsServerHandleType]:
        tags = {
            "dagster/location_hash": deterministic_label_for_location(
                deployment_name, location_name
            ),
            "dagster/grpc_server": "1",
            "dagster/agent_id": self._instance.instance_uuid,
        }
        return self.client.list_services(tags)

    def _list_server_handles(self) -> List[EcsServerHandleType]:
        return [
            service
            for service in self.client.list_services()
            if "dagster/location_name" in service.tags.keys()
        ]

    def get_agent_id_for_server(self, handle: EcsServerHandleType) -> Optional[str]:
        # Need to get container for server handle, then get the agent tag from that.
        return handle.tags.get("dagster/agent_id")

    def _run_launcher_kwargs(self) -> Dict[str, Any]:
        return dict(
            task_definition={
                "log_group": self.log_group,
                "execution_role_arn": self.execution_role_arn,
                "requires_compatibilities": [self.launch_type],
                **({"task_role_arn": self.task_role_arn} if self.task_role_arn else {}),
                **(
                    {"sidecar_containers": self.run_sidecar_containers}
                    if self.run_sidecar_containers
                    else {}
                ),
                **({"runtime_platform": self.runtime_platform} if self.runtime_platform else {}),
                **({"mount_points": self.mount_points} if self.mount_points else {}),
                **({"volumes": self.volumes} if self.volumes else {}),
            },
            secrets=self.secrets,
            secrets_tag=self.secrets_tag,
            env_vars=self.env_vars,
            use_current_ecs_task_config=False,
            run_task_kwargs={
                "cluster": self.cluster,
                "networkConfiguration": self.client.network_configuration,
                "launchType": self.launch_type,
            },
            run_ecs_tags=self.run_ecs_tags,
            container_name=CONTAINER_NAME,
            run_resources=self.run_resources,
        )

    def run_launcher(self) -> CloudEcsRunLauncher:
        launcher = CloudEcsRunLauncher(**self._run_launcher_kwargs())
        launcher.register_instance(self._instance)

        return launcher
