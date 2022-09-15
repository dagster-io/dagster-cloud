import logging
import sys
import time
from typing import Any, Collection, Dict, List, Optional, Tuple

import docker
from dagster import Field, IntSource
from dagster import _check as check
from dagster._core.utils import parse_env_var
from dagster._serdes import ConfigurableClass
from dagster._utils import find_free_port, merge_dicts
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata
from dagster_docker import DockerRunLauncher
from dagster_docker.container_context import DockerContainerContext
from docker.models.containers import Container

from ..config_schema.docker import SHARED_DOCKER_CONFIG
from ..user_code_launcher import (
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
    SHARED_USER_CODE_LAUNCHER_CONFIG,
    DagsterCloudUserCodeLauncher,
    ServerEndpoint,
)
from ..user_code_launcher.utils import deterministic_label_for_location
from .utils import unique_docker_resource_name

GRPC_SERVER_LABEL = "dagster_grpc_server"

IMAGE_PULL_LOG_INTERVAL = 15


class DockerUserCodeLauncher(DagsterCloudUserCodeLauncher[Container], ConfigurableClass):
    def __init__(
        self,
        inst_data=None,
        networks=None,
        env_vars=None,
        container_kwargs=None,
        server_process_startup_timeout=None,
        **kwargs,
    ):
        self._inst_data = inst_data
        self._logger = logging.getLogger("dagster_cloud")
        self._networks = check.opt_list_param(networks, "networks", of_type=str)
        self._input_env_vars = check.opt_list_param(env_vars, "env_vars", of_type=str)

        self._container_kwargs = check.opt_dict_param(
            container_kwargs, "container_kwargs", key_type=str
        )

        self._server_process_startup_timeout = check.opt_int_param(
            server_process_startup_timeout,
            "server_process_startup_timeout",
            DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
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
                    description="Timeout when waiting for a code server to be ready after it is created",
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
        deployment_name,
        location_name,
        metadata,
        container_name,
        hostname,
        environment,
        ports,
        container_context,
        command,
    ):
        return client.containers.create(
            metadata.image,
            detach=True,
            hostname=hostname,
            name=container_name,
            network=container_context.networks[0] if len(container_context.networks) else None,
            environment=environment,
            labels=[
                GRPC_SERVER_LABEL,
                deterministic_label_for_location(deployment_name, location_name),
            ],
            command=command,
            ports=ports,
            **container_context.container_kwargs,
        )

    def _get_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[Container]:
        client = docker.client.from_env()
        return client.containers.list(
            all=True,
            filters={"label": deterministic_label_for_location(deployment_name, location_name)},
        )

    def _start_new_server_spinup(
        self, deployment_name: str, location_name: str, metadata: CodeDeploymentMetadata
    ) -> Tuple[Container, ServerEndpoint]:
        command = metadata.get_grpc_server_command()
        client = docker.client.from_env()

        container_context = DockerContainerContext(
            registry=None,
            env_vars=self.env_vars
            + [f"{k}={v}" for k, v in (metadata.cloud_context_env or {}).items()],
            networks=self._networks,
            container_kwargs=self._container_kwargs,
        ).merge(DockerContainerContext.create_from_config(metadata.container_context))

        container_name = unique_docker_resource_name(deployment_name, location_name)

        self._logger.info(
            "Starting a new container for {deployment_name}:{location_name} with image {image}: {container_name}".format(
                deployment_name=deployment_name,
                location_name=location_name,
                image=metadata.image,
                container_name=container_name,
            )
        )

        ports = {}

        has_network = len(self._networks) > 0
        if has_network:
            grpc_port = 4000
            hostname = container_name
        else:
            grpc_port = find_free_port()
            ports[grpc_port] = grpc_port
            hostname = "localhost"

        environment = merge_dicts(
            (dict([parse_env_var(env_var) for env_var in container_context.env_vars])),
            metadata.get_grpc_server_env(grpc_port),
            {"DAGSTER_SERVER_NAME": container_name},
        )

        try:
            container = self._create_container(
                client,
                deployment_name,
                location_name,
                metadata,
                container_name,
                hostname,
                environment,
                ports,
                container_context,
                command,
            )
        except docker.errors.ImageNotFound:
            last_log_time = time.time()
            self._logger.info("Pulling image {image}...".format(image=metadata.image))
            for _line in docker.APIClient().pull(metadata.image, stream=True):
                if time.time() - last_log_time > IMAGE_PULL_LOG_INTERVAL:
                    self._logger.info("Still pulling image {image}...".format(image=metadata.image))
                    last_log_time = time.time()

            container = self._create_container(
                client,
                deployment_name,
                location_name,
                metadata,
                container_name,
                hostname,
                environment,
                ports,
                container_context,
                command,
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

    def _wait_for_new_server_ready(
        self,
        deployment_name: str,
        location_name: str,
        metadata: CodeDeploymentMetadata,
        server_handle: Container,
        server_endpoint: ServerEndpoint,
    ) -> None:
        self._wait_for_server_process(
            host=server_endpoint.host,
            port=server_endpoint.port,
            timeout=self._server_process_startup_timeout,
            socket=server_endpoint.socket,
        )

    def _remove_server_handle(self, server_handle: Container) -> None:
        container = server_handle
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
            self._remove_server_handle(container)

    def run_launcher(self):
        launcher = DockerRunLauncher(
            image=None,
            env_vars=self.env_vars,
            networks=self._networks,
            container_kwargs=self._container_kwargs,
        )
        launcher.register_instance(self._instance)

        return launcher
