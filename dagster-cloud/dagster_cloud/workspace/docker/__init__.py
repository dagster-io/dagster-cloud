import logging
import sys
import time
from typing import Any, Collection, Dict, List, NamedTuple, Tuple

import docker
from dagster import (
    Field,
    IntSource,
    _check as check,
)
from dagster._core.utils import parse_env_var
from dagster._serdes import ConfigurableClass
from dagster._utils import find_free_port
from dagster._utils.merger import merge_dicts
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata
from dagster_docker import DockerRunLauncher
from dagster_docker.container_context import DockerContainerContext
from docker.models.containers import Container

from ..config_schema.docker import SHARED_DOCKER_CONFIG
from ..user_code_launcher import (
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
    SHARED_USER_CODE_LAUNCHER_CONFIG,
    DagsterCloudGrpcServer,
    DagsterCloudUserCodeLauncher,
    ServerEndpoint,
)
from ..user_code_launcher.utils import deterministic_label_for_location
from .utils import unique_docker_resource_name

GRPC_SERVER_LABEL = "dagster_grpc_server"
MULTIPEX_SERVER_LABEL = "dagster_multipex_server"

IMAGE_PULL_LOG_INTERVAL = 15


class DagsterDockerContainer(NamedTuple):
    """
    We use __str__ on server handles to serialize them to the run tags. Wrap the docker container
    object so that we can serialize it to a string.
    """

    container: Container

    def __str__(self):
        return self.container.id


class DockerUserCodeLauncher(
    DagsterCloudUserCodeLauncher[DagsterDockerContainer], ConfigurableClass
):
    def __init__(
        self,
        inst_data=None,
        networks=None,
        env_vars=None,
        container_kwargs=None,
        **kwargs,
    ):
        self._inst_data = inst_data
        self._logger = logging.getLogger("dagster_cloud")
        self._networks = check.opt_list_param(networks, "networks", of_type=str)
        self._input_env_vars = check.opt_list_param(env_vars, "env_vars", of_type=str)

        self._container_kwargs = check.opt_dict_param(
            container_kwargs, "container_kwargs", key_type=str
        )

        super(DockerUserCodeLauncher, self).__init__(**kwargs)

    @property
    def requires_images(self):
        return True

    @property
    def inst_data(self):
        return self._inst_data

    @property
    def env_vars(self) -> List[str]:
        return self._input_env_vars + self._instance.dagster_cloud_api_env_vars

    @property
    def container_kwargs(self) -> Dict[str, Any]:
        return self._container_kwargs

    @classmethod
    def config_type(cls):
        return merge_dicts(
            SHARED_DOCKER_CONFIG,
            {
                "server_process_startup_timeout": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
                    description=(
                        "Timeout when waiting for a code server to be ready after it is created"
                    ),
                ),
            },
            SHARED_USER_CODE_LAUNCHER_CONFIG,
        )

    @staticmethod
    def from_config_value(inst_data, config_value):
        return DockerUserCodeLauncher(inst_data=inst_data, **config_value)

    def _create_container(
        self,
        client,
        image,
        container_name,
        hostname,
        environment,
        ports,
        container_context,
        command,
        labels,
    ):
        return client.containers.create(
            image,
            detach=True,
            hostname=hostname,
            name=container_name,
            network=container_context.networks[0] if len(container_context.networks) else None,
            environment=environment,
            labels=labels,
            command=command,
            ports=ports,
            **container_context.container_kwargs,
        )

    def _get_standalone_dagster_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[Container]:
        client = docker.client.from_env()
        return [
            DagsterDockerContainer(container=container)
            for container in client.containers.list(
                all=True,
                filters={
                    "label": [
                        GRPC_SERVER_LABEL,
                        deterministic_label_for_location(deployment_name, location_name),
                    ]
                },
            )
        ]

    def _get_multipex_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[Container]:
        client = docker.client.from_env()
        return [
            DagsterDockerContainer(container=container)
            for container in client.containers.list(
                all=True,
                filters={
                    "label": [
                        MULTIPEX_SERVER_LABEL,
                        deterministic_label_for_location(deployment_name, location_name),
                    ]
                },
            )
        ]

    def _launch_container(
        self,
        deployment_name: str,
        location_name: str,
        container_name: str,
        hostname: str,
        grpc_port: int,
        ports: Dict[int, int],
        image: str,
        container_context: DockerContainerContext,
        command: List[str],
        additional_env: Dict[str, str],
        labels: List[str],
    ) -> Tuple[Container, ServerEndpoint]:
        client = docker.client.from_env()

        self._logger.info(
            "Starting a new container for {deployment_name}:{location_name} with image {image}:"
            " {container_name}".format(
                deployment_name=deployment_name,
                location_name=location_name,
                image=image,
                container_name=container_name,
            )
        )

        environment = merge_dicts(
            (dict([parse_env_var(env_var) for env_var in container_context.env_vars])),
            additional_env,
        )

        try:
            container = self._create_container(
                client,
                image,
                container_name,
                hostname,
                environment,
                ports,
                container_context,
                command,
                labels,
            )
        except docker.errors.ImageNotFound:
            last_log_time = time.time()
            self._logger.info("Pulling image {image}...".format(image=image))
            for _line in docker.APIClient().pull(image, stream=True):
                if time.time() - last_log_time > IMAGE_PULL_LOG_INTERVAL:
                    self._logger.info("Still pulling image {image}...".format(image=image))
                    last_log_time = time.time()

            container = self._create_container(
                client,
                image,
                container_name,
                hostname,
                environment,
                ports,
                container_context,
                command,
                labels,
            )

        if len(container_context.networks) > 1:
            for network_name in container_context.networks[1:]:
                network = client.networks.get(network_name)
                network.connect(container)

        container.start()

        endpoint = ServerEndpoint(
            host=hostname,
            port=grpc_port,
            socket=None,
        )

        self._logger.info("Started container {container_id}".format(container_id=container.id))

        return (container, endpoint)

    def _start_new_server_spinup(
        self,
        deployment_name: str,
        location_name: str,
        metadata: CodeDeploymentMetadata,
    ) -> DagsterCloudGrpcServer:
        container_name = unique_docker_resource_name(deployment_name, location_name)

        container_context = DockerContainerContext(
            registry=None,
            env_vars=self.env_vars
            + [f"{k}={v}" for k, v in (metadata.cloud_context_env or {}).items()],
            networks=self._networks,
            container_kwargs=self._container_kwargs,
        ).merge(DockerContainerContext.create_from_config(metadata.container_context))

        ports = {}

        has_network = len(self._networks) > 0
        if has_network:
            grpc_port = 4000
            hostname = container_name
        else:
            grpc_port = find_free_port()
            ports[grpc_port] = grpc_port
            hostname = "localhost"

        if metadata.pex_metadata:
            command = metadata.get_multipex_server_command(grpc_port)
            environment = metadata.get_multipex_server_env()
            labels = [
                MULTIPEX_SERVER_LABEL,
                deterministic_label_for_location(deployment_name, location_name),
            ]
        else:
            command = metadata.get_grpc_server_command()
            environment = metadata.get_grpc_server_env(
                grpc_port, location_name, self._instance.ref_for_deployment(deployment_name)
            )
            labels = [
                GRPC_SERVER_LABEL,
                deterministic_label_for_location(deployment_name, location_name),
            ]

        container, server_endpoint = self._launch_container(
            deployment_name,
            location_name,
            container_name,
            hostname,
            grpc_port,
            ports,
            check.not_none(metadata.image),
            container_context,
            command,
            additional_env=environment,
            labels=labels,
        )

        return DagsterCloudGrpcServer(
            DagsterDockerContainer(container=container), server_endpoint, metadata
        )

    def _wait_for_new_server_ready(
        self,
        deployment_name: str,
        location_name: str,
        metadata: CodeDeploymentMetadata,
        server_handle: DagsterDockerContainer,
        server_endpoint: ServerEndpoint,
    ) -> None:
        self._wait_for_dagster_server_process(
            client=server_endpoint.create_client(),
            timeout=self._server_process_startup_timeout,
        )

    def _remove_server_handle(self, server_handle: DagsterDockerContainer) -> None:
        container = server_handle.container
        try:
            container.stop()
        except Exception:
            self._logger.error(
                "Failure stopping container {container_id}: {exc_info}".format(
                    container_id=container.id,
                    exc_info=sys.exc_info(),
                )
            )
        container.remove(force=True)
        self._logger.info("Removed container {container_id}".format(container_id=container.id))

    def _cleanup_servers(self) -> None:
        client = docker.client.from_env()

        containers = client.containers.list(all=True, filters={"label": GRPC_SERVER_LABEL})
        for container in containers:
            self._remove_server_handle(DagsterDockerContainer(container=container))

        containers = client.containers.list(all=True, filters={"label": MULTIPEX_SERVER_LABEL})
        for container in containers:
            self._remove_server_handle(DagsterDockerContainer(container=container))

    def run_launcher(self):
        launcher = DockerRunLauncher(
            image=None,
            env_vars=self.env_vars,
            networks=self._networks,
            container_kwargs=self._container_kwargs,
        )
        launcher.register_instance(self._instance)

        return launcher
