import os
import sys
import uuid
from typing import Dict, Optional

import docker
from dagster import Array, Field, check
from dagster.core.host_representation.grpc_server_registry import GrpcServerEndpoint
from dagster.daemon.daemon import get_default_daemon_logger
from dagster.serdes import ConfigurableClass
from dagster.serdes.utils import create_snapshot_id
from dagster.utils import find_free_port, merge_dicts
from dagster_cloud.execution.step_handler.docker_step_handler import DockerStepHandler
from dagster_cloud.execution.watchful_run_launcher.docker import WatchfulDockerRunLauncher

from .user_code_launcher import ReconcileUserCodeLauncher

GRPC_SERVER_LABEL = "dagster_grpc_server"


class DockerUserCodeLauncher(ReconcileUserCodeLauncher, ConfigurableClass):
    def __init__(
        self,
        inst_data=None,
        networks=None,
        env_vars=None,
    ):
        self._inst_data = inst_data
        self._logger = get_default_daemon_logger("DockerUserCodeLauncher")
        self._networks = check.opt_list_param(networks, "networks", of_type=str)
        self._env_vars = env_vars

        super(DockerUserCodeLauncher, self).__init__()

    def start(self):
        super().start()
        self._cleanup_containers()

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "networks": Field(Array(str), is_required=False),
            "env_vars": Field(
                [str],
                is_required=False,
                description="The list of environment variables names to forward to the docker container",
            ),
        }

    @staticmethod
    def from_config_value(inst_data, config_value):
        return DockerUserCodeLauncher(inst_data=inst_data, **config_value)

    def _create_container(self, client, location_name, metadata, container_name, hostname, port):

        python_env = (
            {"DAGSTER_CLI_API_GRPC_PYTHON_FILE": metadata.python_file}
            if metadata.python_file
            else {"DAGSTER_CLI_API_GRPC_PACKAGE_NAME": metadata.package_name}
        )
        return client.containers.create(
            metadata.image,
            detach=True,
            hostname=hostname,
            name=container_name,
            network=self._networks[0] if len(self._networks) else None,
            environment=merge_dicts(
                (
                    {env_name: os.getenv(env_name) for env_name in self._env_vars}
                    if self._env_vars
                    else {}
                ),
                {
                    "DAGSTER_CURRENT_IMAGE": metadata.image,
                    "PYTHONUNBUFFERED": "1",
                    "DAGSTER_CLI_API_GRPC_PORT": str(port),
                    "DAGSTER_CLI_API_GRPC_HOST": "0.0.0.0",
                    "DAGSTER_CLI_API_GRPC_LAZY_LOAD_USER_CODE": "1",
                },
                python_env,
            ),
            labels=[GRPC_SERVER_LABEL, self._get_label_for_metadata(location_name, metadata)],
            command=["dagster", "api", "grpc"],
            ports={port: port} if hostname == "localhost" else None,
        )

    def _get_containers(self, client, location_name, metadata):
        return client.containers.list(
            all=True, filters={"label": self._get_label_for_metadata(location_name, metadata)}
        )

    def _add_server(self, location_name, metadata):
        client = docker.client.from_env()

        # check if the container already exists, remove it if so (could happen
        # if a previous attempt to add the container failed)
        existing_containers = self._get_containers(client, location_name, metadata)
        for existing_container in existing_containers:
            self._logger.info(
                "Removing existing container for location {location_name} with image {image}: {container_name}".format(
                    location_name=location_name,
                    image=metadata.image,
                    container_name=existing_container.name,
                )
            )
            self._remove_container(existing_container)

        return self._start_new_server(location_name, metadata)

    def _start_new_server(self, location_name, metadata):
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
            client.images.pull(metadata.image)
            container = self._create_container(
                client, location_name, metadata, container_name, hostname, port
            )

        if len(self._networks) > 1:
            for network_name in self._networks[1:]:
                network = client.networks.get(network_name)
                network.connect(container)

        container.start()

        self._logger.info("Started container {container_id}".format(container_id=container.id))

        server_id = self._wait_for_server(host=hostname, port=port)

        endpoint = GrpcServerEndpoint(
            server_id=server_id,
            host=hostname,
            port=port,
            socket=None,
        )

        return endpoint

    def _gen_update_server(self, location_name, old_metadata, new_metadata):
        client = docker.client.from_env()
        existing_containers = self._get_containers(client, location_name, old_metadata)
        updated_server = self._start_new_server(location_name, new_metadata)
        yield updated_server

        # Now cleanup the old containers
        for existing_container in existing_containers:
            self._logger.info(
                "Removing old container for location {location_name}: {container_name}".format(
                    location_name=location_name,
                    container_name=existing_container.name,
                )
            )
            self._remove_container(existing_container)

    def _get_label_for_metadata(self, location_name, metadata):
        return f"{location_name}_{create_snapshot_id(metadata)[0:6]}"

    def _remove_server(self, location_name, metadata):
        client = docker.client.from_env()
        containers = self._get_containers(client, location_name, metadata)
        for container in containers:
            self._logger.info(
                "Removing a container for location {location_name} with image {image}: {container_name}".format(
                    location_name=location_name,
                    image=container.image,
                    container_name=container.name,
                )
            )

            self._remove_container(container)

    def _remove_container(self, container):
        try:
            container.stop()
        except Exception:  # pylint: disable=broad-except
            self._logger.error(
                "Failure stopping container {container_id}: {exc_info}".format(
                    container_id=container.id,
                    exc_info=sys.exc_info(),
                )
            )
        container.remove(force=True)
        self._logger.info("Removed container {container_id}".format(container_id=container.id))

    def _cleanup_containers(self):
        client = docker.client.from_env()
        # Shut down any dangling containers with the gRPC label
        existing_containers = client.containers.list(all=True, filters={"label": GRPC_SERVER_LABEL})

        for container in existing_containers:
            self._logger.info(
                "Stopping and removing existing gRPC container {container_name}".format(
                    container_name=container.name
                )
            )
            self._remove_container(container)

    def __exit__(self, exception_type, exception_value, traceback):
        super().__exit__(exception_value, exception_value, traceback)
        self._cleanup_containers()

    def get_step_handler(self, _execution_config: Optional[Dict]) -> DockerStepHandler:
        return DockerStepHandler(self._networks, self._env_vars)

    def run_launcher(self):
        launcher = WatchfulDockerRunLauncher(
            image=None,
            env_vars=self._env_vars,
            networks=self._networks,
        )
        launcher.register_instance(self._instance)

        return launcher
