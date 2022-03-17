import logging
import os
import sys
import time
import uuid
from typing import Any, Collection, Dict, List, Optional

import docker
from dagster import Array, Field, IntSource, Permissive, check
from dagster.core.host_representation.grpc_server_registry import GrpcServerEndpoint
from dagster.serdes import ConfigurableClass
from dagster.utils import find_free_port, merge_dicts
from dagster_cloud.execution.step_handler.docker_step_handler import DockerStepHandler
from dagster_cloud.workspace.origin import CodeDeploymentMetadata
from dagster_docker import DockerRunLauncher
from docker.models.containers import Container

from .user_code_launcher import DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT, ReconcileUserCodeLauncher

GRPC_SERVER_LABEL = "dagster_grpc_server"

IMAGE_PULL_LOG_INTERVAL = 15


class DockerUserCodeLauncher(ReconcileUserCodeLauncher[Container], ConfigurableClass):
    def __init__(
        self,
        inst_data=None,
        networks=None,
        env_vars=None,
        container_kwargs=None,
        server_process_startup_timeout=None,
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

        super(DockerUserCodeLauncher, self).__init__()

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
        return {
            "networks": Field(Array(str), is_required=False),
            "env_vars": Field(
                [str],
                is_required=False,
                description="The list of environment variables names to forward to the docker container",
            ),
            "container_kwargs": Field(
                Permissive(),
                is_required=False,
                description="key-value pairs that can be passed into containers.create. See "
                "https://docker-py.readthedocs.io/en/stable/containers.html for the full list "
                "of available options.",
            ),
            "server_process_startup_timeout": Field(
                IntSource,
                is_required=False,
                default_value=DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
                description="Timeout when waiting for a code server to be ready after it is created",
            ),
        }

    @staticmethod
    def from_config_value(inst_data, config_value):
        return DockerUserCodeLauncher(inst_data=inst_data, **config_value)

    def _create_container(self, client, location_name, metadata, container_name, hostname, port):
        return client.containers.create(
            metadata.image,
            detach=True,
            hostname=hostname,
            name=container_name,
            network=self._networks[0] if len(self._networks) else None,
            environment=merge_dicts(
                ({env_name: os.getenv(env_name) for env_name in self.env_vars}),
                metadata.get_grpc_server_env(port),
            ),
            labels=[GRPC_SERVER_LABEL, self._get_label_for_location(location_name)],
            command=metadata.get_grpc_server_command(),
            ports={port: port} if hostname == "localhost" else None,
            **self._container_kwargs,
        )

    def _get_server_handles_for_location(self, location_name: str) -> Collection[Container]:
        client = docker.client.from_env()
        return client.containers.list(
            all=True, filters={"label": self._get_label_for_location(location_name)}
        )

    def _create_new_server_endpoint(
        self, location_name: str, metadata: CodeDeploymentMetadata
    ) -> GrpcServerEndpoint:
        client = docker.client.from_env()

        container_name = f"{location_name}_{str(uuid.uuid4().hex)[0:6]}"

        self._logger.info(
            "Starting a new container for location {location_name} with image {image}: {container_name}".format(
                location_name=location_name, image=metadata.image, container_name=container_name
            )
        )

        has_network = len(self._networks) > 0
        if has_network:
            port = 4000
            hostname = container_name
        else:
            port = find_free_port()
            hostname = "localhost"

        try:
            container = self._create_container(
                client, location_name, metadata, container_name, hostname, port
            )
        except docker.errors.ImageNotFound:
            last_log_time = time.time()
            self._logger.info("Pulling image {image}...".format(image=metadata.image))
            for _line in docker.APIClient().pull(metadata.image, stream=True):
                if time.time() - last_log_time > IMAGE_PULL_LOG_INTERVAL:
                    self._logger.info("Still pulling image {image}...".format(image=metadata.image))
                    last_log_time = time.time()

            container = self._create_container(
                client, location_name, metadata, container_name, hostname, port
            )

        if len(self._networks) > 1:
            for network_name in self._networks[1:]:
                network = client.networks.get(network_name)
                network.connect(container)

        container.start()

        self._logger.info("Started container {container_id}".format(container_id=container.id))

        server_id = self._wait_for_server(
            host=hostname, port=port, timeout=self._server_process_startup_timeout
        )

        endpoint = GrpcServerEndpoint(
            server_id=server_id,
            host=hostname,
            port=port,
            socket=None,
        )

        return endpoint

    def _get_label_for_location(self, location_name):
        return f"location_{location_name}"

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

    def get_step_handler(self, _execution_config: Optional[Dict]) -> DockerStepHandler:
        return DockerStepHandler(self._networks, self.env_vars, self._container_kwargs)

    def run_launcher(self):
        launcher = DockerRunLauncher(
            image=None,
            env_vars=self.env_vars,
            networks=self._networks,
            container_kwargs=self._container_kwargs,
        )
        launcher.register_instance(self._instance)

        return launcher
