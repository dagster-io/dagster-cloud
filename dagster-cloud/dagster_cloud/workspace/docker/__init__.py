import logging
import os
import sys
import time
import uuid
from typing import Any, Collection, Dict, List, Optional

import docker
from dagster import Array, Field, IntSource, Permissive
from dagster import _check as check
from dagster.core.host_representation.grpc_server_registry import GrpcServerEndpoint
from dagster.serdes import ConfigurableClass
from dagster.utils import find_free_port, merge_dicts
from dagster_cloud.api.dagster_cloud_api import (
    DagsterCloudSandboxConnectionInfo,
    DagsterCloudSandboxProxyInfo,
)
from dagster_cloud.execution.step_handler.docker_step_handler import DockerStepHandler
from dagster_cloud.workspace.origin import CodeDeploymentMetadata
from dagster_docker import DockerRunLauncher
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import parse_env_var
from docker.models.containers import Container

from ..config_schema.docker import SHARED_DOCKER_CONFIG
from ..user_code_launcher import (
    DAGSTER_SANDBOX_PORT_ENV,
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
    DagsterCloudUserCodeLauncher,
    find_unallocated_sandbox_port,
)

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
        )

    @staticmethod
    def from_config_value(inst_data, config_value):
        return DockerUserCodeLauncher(inst_data=inst_data, **config_value)

    def _create_container(
        self,
        client,
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
            labels=[GRPC_SERVER_LABEL, self._get_label_for_location(location_name)],
            command=command,
            ports=ports,
            **container_context.container_kwargs,
        )

    def _get_server_handles_for_location(self, location_name: str) -> Collection[Container]:
        client = docker.client.from_env()
        return client.containers.list(
            all=True, filters={"label": self._get_label_for_location(location_name)}
        )

    @property
    def supports_dev_sandbox(self) -> bool:
        return True

    def _create_dev_sandbox_endpoint(
        self,
        location_name: str,
        metadata: CodeDeploymentMetadata,
        authorized_key: str,
        proxy_info: DagsterCloudSandboxProxyInfo,
    ) -> GrpcServerEndpoint:
        port = find_unallocated_sandbox_port(
            allocated_ports=list_allocated_sandbox_ports(),
            proxy_info=proxy_info,
        )

        return self._launch(
            location_name=location_name,
            metadata=metadata,
            command=["supervisord"],
            additional_environment={  # TODO: Abstract into SandboxContainerEnvironment
                "DAGSTER_SANDBOX_AUTHORIZED_KEY": authorized_key,
                "DAGSTER_PROXY_HOSTNAME": proxy_info.hostname,
                "DAGSTER_PROXY_PORT": str(proxy_info.port),
                "DAGSTER_PROXY_AUTH_TOKEN": proxy_info.auth_token,
                DAGSTER_SANDBOX_PORT_ENV: port,
            },
        )

    def get_sandbox_connection_info(self, location_name: str) -> DagsterCloudSandboxConnectionInfo:
        # TODO: Re-implement this in the base class. We'll need to extend handles to include
        # info like the environment and created timestamp or add abstract methods to look
        # this information up.

        # If there are multiple handles for a location,
        # get the most recently created one. We assume this
        # is the "new" one in our blue/green deployment.
        containers = sorted(
            list(self._get_server_handles_for_location(location_name)),
            key=lambda handle: handle.attrs["Created"],
        )

        try:
            container = containers[-1]
        except IndexError:
            raise Exception(f"{location_name} has no running containers.")

        port_env = next(
            (env for env in container.attrs["Config"]["Env"] if "DAGSTER_SANDBOX_PORT" in env)
        )
        port = port_env.split("=")[1]

        hostname_env = next(
            (env for env in container.attrs["Config"]["Env"] if "DAGSTER_PROXY_HOSTNAME" in env)
        )
        hostname = hostname_env.split("=")[1]

        username = "root"  # Hardcoded for now

        return DagsterCloudSandboxConnectionInfo(
            username=username,
            hostname=hostname,
            port=int(port),
        )

    def _create_new_server_endpoint(
        self, location_name: str, metadata: CodeDeploymentMetadata
    ) -> GrpcServerEndpoint:
        return self._launch(
            location_name=location_name,
            metadata=metadata,
            command=metadata.get_grpc_server_command(),
        )

    def _launch(
        self,
        location_name: str,
        metadata: CodeDeploymentMetadata,
        command: List[str],
        additional_environment: Optional[Dict[str, str]] = None,
    ) -> GrpcServerEndpoint:
        client = docker.client.from_env()

        container_context = DockerContainerContext(
            registry=None,
            env_vars=self.env_vars,
            networks=self._networks,
            container_kwargs=self._container_kwargs,
        ).merge(DockerContainerContext.create_from_config(metadata.container_context))

        container_name = f"{location_name}_{str(uuid.uuid4().hex)[0:6]}"

        self._logger.info(
            "Starting a new container for location {location_name} with image {image}: {container_name}".format(
                location_name=location_name, image=metadata.image, container_name=container_name
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
            additional_environment or {},
            {"DAGSTER_SERVER_NAME": container_name},
        )

        try:
            container = self._create_container(
                client,
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

        self._logger.info("Started container {container_id}".format(container_id=container.id))

        server_id = self._wait_for_server(
            host=hostname, port=grpc_port, timeout=self._server_process_startup_timeout
        )

        endpoint = GrpcServerEndpoint(
            server_id=server_id,
            host=hostname,
            port=grpc_port,
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


def list_allocated_sandbox_ports() -> List[int]:
    client = docker.client.from_env()
    containers = client.containers.list(
        filters={
            "label": GRPC_SERVER_LABEL,
        },
    )

    allocated_ports = []
    for container in containers:
        env_list = container.attrs["Config"]["Env"]
        env = dict(item.split("=", 1) for item in env_list)
        port = env.get("DAGSTER_SANDBOX_PORT")
        if port:
            allocated_ports.append(int(port))

    return sorted(allocated_ports)
