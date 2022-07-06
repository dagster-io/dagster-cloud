import logging
from contextlib import contextmanager
from typing import Collection, Dict, List, Optional

import kubernetes
import kubernetes.client as client
from dagster import Field, IntSource, Noneable, StringSource
from dagster import _check as check
from dagster.core.host_representation.grpc_server_registry import GrpcServerEndpoint
from dagster.serdes import ConfigurableClass
from dagster.utils import merge_dicts
from dagster_cloud.execution.cloud_run_launcher.k8s import CloudK8sRunLauncher
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata
from dagster_k8s.container_context import K8sContainerContext
from dagster_k8s.models import k8s_snake_case_dict
from kubernetes.client.rest import ApiException

from ..user_code_launcher import (
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
    SHARED_USER_CODE_LAUNCHER_CONFIG,
    DagsterCloudUserCodeLauncher,
)
from ..user_code_launcher.utils import deterministic_label_for_location
from .utils import (
    SERVICE_PORT,
    construct_repo_location_deployment,
    construct_repo_location_service,
    unique_k8s_resource_name,
    wait_for_deployment_complete,
)

DEFAULT_DEPLOYMENT_STARTUP_TIMEOUT = 300
DEFAULT_IMAGE_PULL_GRACE_PERIOD = 30

from ..config_schema.kubernetes import SHARED_K8S_CONFIG


class K8sUserCodeLauncher(DagsterCloudUserCodeLauncher[str], ConfigurableClass):
    def __init__(
        self,
        dagster_home,
        instance_config_map,
        inst_data=None,
        namespace=None,
        kubeconfig_file=None,
        pull_policy=None,
        env_config_maps=None,
        env_secrets=None,
        env_vars=None,
        service_account_name=None,
        volume_mounts=None,
        volumes=None,
        image_pull_secrets=None,
        deployment_startup_timeout=None,
        server_process_startup_timeout=None,
        image_pull_grace_period=None,
        labels=None,
        resources=None,
        server_ttl=None,
    ):
        self._inst_data = inst_data
        self._logger = logging.getLogger("K8sUserCodeLauncher")
        self._dagster_home = check.str_param(dagster_home, "dagster_home")
        self._instance_config_map = check.str_param(instance_config_map, "instance_config_map")
        self._namespace = namespace

        self._pull_policy = pull_policy
        self._env_config_maps = check.opt_list_param(
            env_config_maps, "env_config_maps", of_type=str
        )
        self._env_secrets = check.opt_list_param(env_secrets, "env_secrets", of_type=str)
        self._env_vars = check.opt_list_param(env_vars, "env_vars", of_type=str)
        self._service_account_name = check.str_param(service_account_name, "service_account_name")
        self._volume_mounts = [
            k8s_snake_case_dict(kubernetes.client.V1VolumeMount, mount)
            for mount in check.opt_list_param(volume_mounts, "volume_mounts")
        ]
        self._volumes = [
            k8s_snake_case_dict(kubernetes.client.V1Volume, volume)
            for volume in check.opt_list_param(volumes, "volumes")
        ]
        self._image_pull_secrets = check.opt_list_param(
            image_pull_secrets, "image_pull_secrets", of_type=dict
        )
        self._deployment_startup_timeout = check.opt_int_param(
            deployment_startup_timeout,
            "deployment_startup_timeout",
            DEFAULT_DEPLOYMENT_STARTUP_TIMEOUT,
        )
        self._server_process_startup_timeout = check.opt_int_param(
            server_process_startup_timeout,
            "server_process_startup_timeout",
            DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
        )
        self._image_pull_grace_period = check.opt_int_param(
            image_pull_grace_period,
            "image_pull_grace_period",
            DEFAULT_IMAGE_PULL_GRACE_PERIOD,
        )
        self._labels = check.opt_dict_param(labels, "labels", key_type=str, value_type=str)
        self._resources = check.opt_dict_param(resources, "resources", key_type=str)

        if kubeconfig_file:
            kubernetes.config.load_kube_config(kubeconfig_file)
        else:
            kubernetes.config.load_incluster_config()

        super(K8sUserCodeLauncher, self).__init__(server_ttl=server_ttl)

        self._launcher = CloudK8sRunLauncher(
            dagster_home=self._dagster_home,
            instance_config_map=self._instance_config_map,
            postgres_password_secret=None,
            job_image=None,
            image_pull_policy=self._pull_policy,
            image_pull_secrets=self._image_pull_secrets,
            service_account_name=self._service_account_name,
            env_config_maps=self._env_config_maps,
            env_secrets=self._env_secrets,
            env_vars=self._env_vars,
            job_namespace=self._namespace,
            volume_mounts=self._volume_mounts,
            volumes=self._volumes,
            labels=self._labels,
            resources=self._resources,
        )

    @property
    def requires_images(self):
        return True

    def register_instance(self, instance):
        super().register_instance(instance)
        self._launcher.register_instance(instance)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):

        container_context_config = SHARED_K8S_CONFIG.copy()
        del container_context_config["image_pull_policy"]  # uses 'pull_policy'
        del container_context_config["namespace"]  # default is different

        return merge_dicts(
            container_context_config,
            {
                "dagster_home": Field(StringSource, is_required=True),
                "instance_config_map": Field(StringSource, is_required=True),
                "kubeconfig_file": Field(StringSource, is_required=False),
                "deployment_startup_timeout": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_DEPLOYMENT_STARTUP_TIMEOUT,
                    description="Timeout when creating a new Kubernetes deployment for a code server",
                ),
                "server_process_startup_timeout": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
                    description="Timeout when waiting for a code server to be ready after it is created",
                ),
                "image_pull_grace_period": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_IMAGE_PULL_GRACE_PERIOD,
                ),
                "pull_policy": Field(
                    Noneable(StringSource),
                    is_required=False,
                    description="Image pull policy to set on launched Pods.",
                ),
                "namespace": Field(StringSource, is_required=False, default_value="default"),
            },
            SHARED_USER_CODE_LAUNCHER_CONFIG,
        )

    @staticmethod
    def from_config_value(inst_data, config_value):
        return K8sUserCodeLauncher(
            inst_data=inst_data,
            **config_value,
        )

    @contextmanager
    def _get_api_instance(self):
        with client.ApiClient() as api_client:  # pylint: disable=not-context-manager
            yield client.AppsV1Api(api_client)

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
        additional_environment: Optional[Dict[str, str]] = None,
    ) -> GrpcServerEndpoint:
        resource_name = unique_k8s_resource_name(deployment_name, location_name)

        container_context = K8sContainerContext(
            image_pull_policy=self._pull_policy,
            image_pull_secrets=self._image_pull_secrets,
            service_account_name=self._service_account_name,
            env_config_maps=self._env_config_maps,
            env_secrets=self._env_secrets,
            env_vars=self._env_vars,
            volume_mounts=self._volume_mounts,
            volumes=self._volumes,
            labels=self._labels,
            namespace=self._namespace,
            resources=self._resources,
        ).merge(K8sContainerContext.create_from_config(metadata.container_context))

        try:
            with self._get_api_instance() as api_instance:
                api_response = api_instance.create_namespaced_deployment(
                    container_context.namespace,
                    construct_repo_location_deployment(
                        deployment_name,
                        location_name,
                        resource_name,
                        metadata,
                        container_context.image_pull_policy,
                        container_context.env_config_maps,
                        container_context.env_secrets,
                        container_context.service_account_name,
                        container_context.image_pull_secrets,
                        container_context.volume_mounts,
                        container_context.volumes,
                        container_context.labels,
                        container_context.resources,
                        command=command,
                        env=merge_dicts(
                            container_context.get_environment_dict(),
                            (additional_environment or {}),
                        ),
                    ),
                )
            self._logger.info("Created deployment: {}".format(api_response.metadata.name))
        except ApiException as e:
            self._logger.error(
                "Exception when calling AppsV1Api->create_namespaced_deployment: %s\n" % e
            )
            raise e

        try:
            api_response = client.CoreV1Api().create_namespaced_service(
                self._namespace,
                construct_repo_location_service(deployment_name, location_name, resource_name),
            )
            self._logger.info("Created service: {}".format(api_response.metadata.name))
        except ApiException as e:
            self._logger.error(
                "Exception when calling AppsV1Api->create_namespaced_service: %s\n" % e
            )
            raise e

        wait_for_deployment_complete(
            resource_name,
            self._namespace,
            self._logger,
            location_name,
            metadata,
            existing_pods=[],
            timeout=self._deployment_startup_timeout,
            image_pull_grace_period=self._image_pull_grace_period,
        )
        server_id = self._wait_for_server(
            host=resource_name, port=SERVICE_PORT, timeout=self._server_process_startup_timeout
        )

        endpoint = GrpcServerEndpoint(
            server_id=server_id,
            host=resource_name,
            port=SERVICE_PORT,
            socket=None,
        )

        return endpoint

    def _get_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[str]:
        with self._get_api_instance() as api_instance:
            deployments = api_instance.list_namespaced_deployment(
                self._namespace,
                label_selector=f"location_hash={deterministic_label_for_location(deployment_name, location_name)}",
            ).items
            return [deployment.metadata.name for deployment in deployments]

    def _cleanup_servers(self) -> None:
        with self._get_api_instance() as api_instance:
            deployments = api_instance.list_namespaced_deployment(
                self._namespace,
                label_selector="managed_by=K8sUserCodeLauncher",
            ).items
            for deployment in deployments:
                self._remove_server_handle(deployment.metadata.name)

    def _remove_server_handle(self, server_handle: str) -> None:
        check.str_param(server_handle, "server_handle")
        with self._get_api_instance() as api_instance:
            api_instance.delete_namespaced_deployment(server_handle, self._namespace)
        client.CoreV1Api().delete_namespaced_service(server_handle, self._namespace)
        self._logger.info("Removed deployment and service: {}".format(server_handle))

    def __exit__(self, exception_type, exception_value, traceback):
        super().__exit__(exception_value, exception_value, traceback)
        self._launcher.dispose()

    def run_launcher(self):
        return self._launcher
