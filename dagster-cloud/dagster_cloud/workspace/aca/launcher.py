from __future__ import annotations

import asyncio
import os
from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING, Any

import dagster._check as check
from dagster import Field, IntSource, Noneable, StringSource
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils.merger import merge_dicts

from dagster_cloud.api.dagster_cloud_api import UserCodeDeploymentType
from dagster_cloud.execution.monitoring import CloudContainerResourceLimits
from dagster_cloud.workspace.aca.container_context import AcaContainerContext
from dagster_cloud.workspace.aca.handle import AcaServerHandle
from dagster_cloud.workspace.aca.run_launcher import AcaRunLauncher
from dagster_cloud.workspace.aca.utils import (
    get_aca_human_readable_label,
    unique_aca_resource_name,
)
from dagster_cloud.workspace.config_schema import SHARED_ACA_CONFIG
from dagster_cloud.workspace.user_code_launcher import (
    DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
    SHARED_USER_CODE_LAUNCHER_CONFIG,
    DagsterCloudGrpcServer,
    DagsterCloudUserCodeLauncher,
    ServerEndpoint,
)
from dagster_cloud.workspace.user_code_launcher.user_code_launcher import (
    UserCodeLauncherEntry,
    async_serialize_exceptions,
)
from dagster_cloud.workspace.user_code_launcher.utils import (
    deterministic_label_for_location,
    get_code_server_port,
    get_grpc_server_env,
)

if TYPE_CHECKING:
    from azure.mgmt.appcontainers import ContainerAppsAPIClient

GRPC_PORT = 50051
_DEFAULT_SERVER_POLL_INTERVAL_SECS = 5
_DEFAULT_SERVER_STARTUP_TIMEOUT_SECS = 180


class AcaUserCodeLauncher(DagsterCloudUserCodeLauncher[AcaServerHandle], ConfigurableClass):
    """User code launcher that runs Dagster code servers as Azure Container Apps.

    Each code location gets a long-lived Container App that serves gRPC requests
    from the agent. Run workers are ephemeral Container Apps created per-run by
    :class:`AcaRunLauncher`.
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        environment_name: str,
        location: str = "eastus",
        env_vars: list[str] | None = None,
        server_resources: dict[str, Any] | None = None,
        run_resources: dict[str, Any] | None = None,
        code_server_identity_id: str | None = None,
        registry_server: str | None = None,
        registry_username: str | None = None,
        registry_password_secret_name: str | None = None,
        inst_data: ConfigurableClassData | None = None,
        **kwargs,
    ):
        self._subscription_id = subscription_id
        self._resource_group = resource_group
        self._environment_name = environment_name
        self._location = location
        self.env_vars = check.opt_list_param(env_vars, "env_vars")
        self.server_resources = check.opt_mapping_param(server_resources, "server_resources")
        self.run_resources = check.opt_mapping_param(run_resources, "run_resources")
        self._code_server_identity_id = code_server_identity_id
        self._registry_server = registry_server
        self._registry_username = registry_username
        self._registry_password_secret_name = registry_password_secret_name
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

        # Lazily initialized after agent starts (avoids auth at import time)
        self._aca_client: ContainerAppsAPIClient | None = None
        self._environment_id: str | None = None
        self._default_domain: str | None = None

        super().__init__(**kwargs)

    # ------------------------------------------------------------------
    # Azure client helpers
    # ------------------------------------------------------------------

    def _get_aca_client(self) -> ContainerAppsAPIClient:
        if self._aca_client is None:
            from azure.identity import DefaultAzureCredential
            from azure.mgmt.appcontainers import ContainerAppsAPIClient

            self._aca_client = ContainerAppsAPIClient(
                DefaultAzureCredential(), self._subscription_id
            )
        return self._aca_client

    def _get_environment_info(self) -> tuple[str, str]:
        """Return (environment_id, default_domain), fetching from ACA once and caching."""
        if self._environment_id is None or self._default_domain is None:
            client = self._get_aca_client()
            env = client.managed_environments.get(self._resource_group, self._environment_name)
            self._environment_id = check.not_none(env.id, "ACA environment has no id")
            self._default_domain = check.not_none(
                env.properties.default_domain, "ACA environment has no default_domain"
            )
        return self._environment_id, self._default_domain

    def _app_hostname(self, app_name: str) -> str:
        _, default_domain = self._get_environment_info()
        return f"{app_name}.{default_domain}"

    # ------------------------------------------------------------------
    # ConfigurableClass interface
    # ------------------------------------------------------------------

    @classmethod
    def config_type(cls):
        return merge_dicts(
            {
                "subscription_id": Field(
                    StringSource,
                    description="Azure subscription ID.",
                ),
                "resource_group": Field(
                    StringSource,
                    description="Azure resource group containing the ACA environment.",
                ),
                "environment_name": Field(
                    StringSource,
                    description="Name of the Azure Container Apps managed environment.",
                ),
                "location": Field(
                    StringSource,
                    is_required=False,
                    default_value="eastus",
                    description="Azure region for Container Apps (e.g. 'eastus', 'westeurope').",
                ),
                "env_vars": Field(
                    [StringSource],
                    is_required=False,
                    description=(
                        "List of environment variable names to forward to all code servers and "
                        "run workers. Each can be KEY=VALUE or a bare KEY (pulled from the "
                        "agent's environment)."
                    ),
                ),
                "code_server_identity_id": Field(
                    Noneable(StringSource),
                    is_required=False,
                    description=(
                        "Resource ID of a user-assigned managed identity to attach to code server "
                        "and run worker Container Apps. Required for pulling images from ACR "
                        "without explicit credentials."
                    ),
                ),
                "registry_server": Field(
                    Noneable(StringSource),
                    is_required=False,
                    description="Container registry server hostname (e.g. 'myacr.azurecr.io').",
                ),
                "registry_username": Field(
                    Noneable(StringSource),
                    is_required=False,
                    description="Username for container registry authentication.",
                ),
                "registry_password_secret_name": Field(
                    Noneable(StringSource),
                    is_required=False,
                    description=(
                        "Name of the ACA secret holding the registry password. "
                        "The secret value is read from the agent environment at startup."
                    ),
                ),
                "server_process_startup_timeout": Field(
                    IntSource,
                    is_required=False,
                    default_value=DEFAULT_SERVER_PROCESS_STARTUP_TIMEOUT,
                    description=(
                        "Timeout (seconds) when waiting for a code server gRPC process to become "
                        "ready after its Container App starts."
                    ),
                ),
            },
            SHARED_ACA_CONFIG,
            SHARED_USER_CODE_LAUNCHER_CONFIG,
        )

    @classmethod
    def from_config_value(cls, inst_data: ConfigurableClassData, config_value: dict[str, Any]):
        return cls(inst_data=inst_data, **config_value)

    @property
    def inst_data(self) -> ConfigurableClassData | None:
        return self._inst_data

    # ------------------------------------------------------------------
    # DagsterCloudUserCodeLauncher interface
    # ------------------------------------------------------------------

    @property
    def requires_images(self) -> bool:
        return True

    @property
    def user_code_deployment_type(self) -> UserCodeDeploymentType:
        return UserCodeDeploymentType.ACA

    def _write_readiness_sentinel(self) -> None:
        Path("/opt/finished_initial_reconciliation_sentinel.txt").touch(exist_ok=True)
        self._logger.info("Wrote readiness sentinel: agent is ready to serve requests")

    # ------------------------------------------------------------------
    # Container App name helpers
    # ------------------------------------------------------------------

    def _server_app_name(self, deployment_name: str, location_name: str) -> str:
        return unique_aca_resource_name(deployment_name, location_name)

    def _dagster_tags(
        self, deployment_name: str, location_name: str, extra: dict[str, str] | None = None
    ) -> dict[str, str]:
        tags = {
            "dagster-deployment": get_aca_human_readable_label(deployment_name),
            "dagster-location": get_aca_human_readable_label(location_name),
            "dagster-location-hash": deterministic_label_for_location(
                deployment_name, location_name
            ),
            "dagster-agent-id": self._instance.instance_uuid,
            "managed-by": "dagster-cloud-agent",
        }
        if extra:
            tags.update(extra)
        return tags

    # ------------------------------------------------------------------
    # Registry / identity helpers
    # ------------------------------------------------------------------

    def _build_secrets_and_registries(self):
        from azure.mgmt.appcontainers.models import RegistryCredentials, Secret

        secrets = []
        registries = []
        if self._registry_server and self._registry_password_secret_name:
            secrets = [
                Secret(
                    name=self._registry_password_secret_name,
                    value=os.environ.get(self._registry_password_secret_name, ""),
                )
            ]
            registries = [
                RegistryCredentials(
                    server=self._registry_server,
                    username=self._registry_username,
                    password_secret_ref=self._registry_password_secret_name,
                )
            ]
        return secrets, registries

    def _build_identity(self):
        from azure.mgmt.appcontainers.models import ManagedServiceIdentity, UserAssignedIdentity

        if self._code_server_identity_id:
            return ManagedServiceIdentity(
                type="UserAssigned",
                user_assigned_identities={self._code_server_identity_id: UserAssignedIdentity()},
            )
        return None

    # ------------------------------------------------------------------
    # Core lifecycle methods
    # ------------------------------------------------------------------

    def _start_new_server_spinup(
        self,
        deployment_name: str,
        location_name: str,
        desired_entry: UserCodeLauncherEntry,
    ) -> DagsterCloudGrpcServer:
        from azure.mgmt.appcontainers.models import (
            Configuration,
            Container,
            ContainerApp,
            ContainerResources,
            EnvironmentVar,
            Ingress,
            Scale,
            Template,
        )

        metadata = desired_entry.code_location_deploy_data

        # Build gRPC server command and base env
        if metadata.pex_metadata:
            command = metadata.get_multipex_server_command(
                get_code_server_port(),
                metrics_enabled=self._instance.user_code_launcher.code_server_metrics_enabled,
            )
            additional_env = metadata.get_multipex_server_env()
            server_tag = "dagster-multipex-server"
        else:
            command = metadata.get_grpc_server_command(
                metrics_enabled=self._instance.user_code_launcher.code_server_metrics_enabled,
            )
            additional_env = get_grpc_server_env(
                metadata,
                get_code_server_port(),
                location_name,
                self._instance.ref_for_deployment(deployment_name),
            )
            server_tag = "dagster-grpc-server"

        # Build merged container context:
        # launcher env_vars + cloud_context_env (DAGSTER_CLOUD_* system vars)
        # + per-location containerContext.aca env_vars
        container_context = AcaContainerContext(
            env_vars=self.env_vars
            + [f"{k}={v}" for k, v in (metadata.cloud_context_env or {}).items()],
            server_resources=dict(self.server_resources),
            run_resources=dict(self.run_resources),
        ).merge(AcaContainerContext.create_from_config(metadata.container_context))

        env_dict = merge_dicts(
            container_context.get_environment_dict(),
            additional_env,
            {"DAGSTER_GRPC_MAX_WORKERS": os.environ["DAGSTER_GRPC_MAX_WORKERS"]}
            if os.environ.get("DAGSTER_GRPC_MAX_WORKERS")
            else {},
        )
        env_vars = [EnvironmentVar(name=k, value=str(v)) for k, v in env_dict.items()]

        # Resource sizing — per-location overrides take precedence over launcher defaults
        resources = container_context.server_resources
        cpu = float(resources.get("cpu", 0.5))
        memory = resources.get("memory", "1.0Gi")

        environment_id, _ = self._get_environment_info()
        app_name = self._server_app_name(deployment_name, location_name)
        secrets, registries = self._build_secrets_and_registries()
        identity = self._build_identity()

        image = self._resolve_image(metadata)
        if image != metadata.image:
            self._logger.info("Resolved image to %r", image)

        tags = self._dagster_tags(
            deployment_name,
            location_name,
            extra={
                server_tag: "1",
                "dagster-server-timestamp": str(desired_entry.update_timestamp),
            },
        )

        container_app = ContainerApp(
            location=self._location,
            managed_environment_id=environment_id,
            identity=identity,
            configuration=Configuration(
                ingress=Ingress(
                    external=False,
                    target_port=GRPC_PORT,
                    transport="http2",
                    allow_insecure=True,
                ),
                secrets=secrets or None,
                registries=registries or None,
                active_revisions_mode="Single",
            ),
            template=Template(
                containers=[
                    Container(
                        name="server",
                        image=image,
                        resources=ContainerResources(cpu=cpu, memory=memory),
                        env=env_vars,
                        command=command,
                    )
                ],
                scale=Scale(min_replicas=1, max_replicas=1, rules=[]),
            ),
            tags=tags,
        )

        client = self._get_aca_client()
        self._logger.info(
            f"Creating code server Container App {app_name!r} for {deployment_name}:{location_name}"
        )
        poller = client.container_apps.begin_create_or_update(
            self._resource_group, app_name, container_app
        )
        app = poller.result(timeout=_DEFAULT_SERVER_STARTUP_TIMEOUT_SECS)

        hostname = self._app_hostname(app_name)
        system_data = getattr(app, "system_data", None)
        created_at = getattr(system_data, "created_at", None) if system_data else None
        create_timestamp = created_at.timestamp() if created_at else None

        handle = AcaServerHandle(
            app_name=app_name,
            hostname=hostname,
            tags=tags,
            create_timestamp=create_timestamp,
        )
        endpoint = ServerEndpoint(host=hostname, port=get_code_server_port(), socket=None)

        self._logger.info(
            f"Created code server {app_name!r} at {hostname} for {deployment_name}:{location_name}"
        )
        return DagsterCloudGrpcServer(handle, endpoint, metadata)

    @async_serialize_exceptions
    async def _wait_for_new_server_ready(
        self,
        deployment_name: str,
        location_name: str,
        user_code_launcher_entry: UserCodeLauncherEntry,
        server_handle: AcaServerHandle,
        server_endpoint: ServerEndpoint,
    ) -> None:
        self._logger.info(
            f"Waiting for Container App {server_handle.app_name!r} to be ready..."
        )
        await self._poll_until_running(server_handle.app_name)
        await self._wait_for_dagster_server_process(
            client=server_endpoint.create_client(),
            timeout=self._server_process_startup_timeout,
            additional_check=lambda: self._assert_app_not_stopped(server_handle.app_name),
        )

    async def _poll_until_running(self, app_name: str) -> None:
        """Poll the Container App until it reports RunningStatus == 'Running'."""
        client = self._get_aca_client()
        deadline = asyncio.get_event_loop().time() + _DEFAULT_SERVER_STARTUP_TIMEOUT_SECS
        while True:
            app = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.container_apps.get(self._resource_group, app_name),
            )
            status = getattr(getattr(app, "properties", None), "running_status", None)
            if status == "Running":
                return
            if status in ("Stopped", "Failed", "Degraded"):
                raise Exception(
                    f"Container App {app_name!r} entered status {status!r} instead of Running."
                )
            if asyncio.get_event_loop().time() > deadline:
                raise Exception(
                    f"Timed out waiting for Container App {app_name!r} to reach Running status "
                    f"(last status: {status!r})."
                )
            await asyncio.sleep(_DEFAULT_SERVER_POLL_INTERVAL_SECS)

    def _assert_app_not_stopped(self, app_name: str) -> None:
        client = self._get_aca_client()
        app = client.container_apps.get(self._resource_group, app_name)
        status = getattr(getattr(app, "properties", None), "running_status", None)
        if status in ("Stopped", "Failed", "Degraded"):
            raise Exception(
                f"Container App {app_name!r} unexpectedly stopped (status: {status!r})."
            )

    def _remove_server_handle(self, server_handle: AcaServerHandle) -> None:
        self._logger.info(f"Deleting Container App {server_handle.app_name!r}...")
        try:
            self._get_aca_client().container_apps.begin_delete(
                self._resource_group, server_handle.app_name
            )
            self._logger.info(f"Deleted Container App {server_handle.app_name!r}.")
        except Exception:
            self._logger.exception(
                f"Failed to delete Container App {server_handle.app_name!r}"
            )

    def _list_server_handles(self) -> list[AcaServerHandle]:
        client = self._get_aca_client()
        handles = []
        for app in client.container_apps.list_by_resource_group(self._resource_group):
            tags = app.tags or {}
            if "dagster-location" not in tags:
                continue
            if tags.get("dagster-component") == "run-worker":
                continue
            system_data = getattr(app, "system_data", None)
            created_at = getattr(system_data, "created_at", None) if system_data else None
            handles.append(
                AcaServerHandle(
                    app_name=app.name,
                    hostname=self._app_hostname(app.name),
                    tags=dict(tags),
                    create_timestamp=created_at.timestamp() if created_at else None,
                )
            )
        return handles

    def _get_standalone_dagster_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[AcaServerHandle]:
        location_hash = deterministic_label_for_location(deployment_name, location_name)
        return [
            h
            for h in self._list_server_handles()
            if h.tags.get("dagster-location-hash") == location_hash
            and "dagster-grpc-server" in h.tags
            and h.tags.get("dagster-agent-id") == self._instance.instance_uuid
        ]

    def _get_multipex_server_handles_for_location(
        self, deployment_name: str, location_name: str
    ) -> Collection[AcaServerHandle]:
        location_hash = deterministic_label_for_location(deployment_name, location_name)
        return [
            h
            for h in self._list_server_handles()
            if h.tags.get("dagster-location-hash") == location_hash
            and "dagster-multipex-server" in h.tags
            and h.tags.get("dagster-agent-id") == self._instance.instance_uuid
        ]

    def get_agent_id_for_server(self, handle: AcaServerHandle) -> str | None:
        return handle.tags.get("dagster-agent-id")

    def get_server_create_timestamp(self, handle: AcaServerHandle) -> float | None:
        return handle.create_timestamp

    def get_code_server_resource_limits(
        self, deployment_name: str, location_name: str
    ) -> CloudContainerResourceLimits:
        metadata = self._actual_entries[(deployment_name, location_name)].code_location_deploy_data
        resources = (metadata.container_context or {}).get("aca", {}).get("server_resources", {})
        return {
            "aca": {
                "cpu_limit": resources.get("cpu"),
                "memory_limit": resources.get("memory"),
            }
        }

    # ------------------------------------------------------------------
    # Run launcher
    # ------------------------------------------------------------------

    def _run_launcher_kwargs(self) -> dict[str, Any]:
        environment_id, _ = self._get_environment_info()
        return {
            "subscription_id": self._subscription_id,
            "resource_group": self._resource_group,
            "environment_id": environment_id,
            "location": self._location,
            "env_vars": self.env_vars,
            "run_resources": dict(self.run_resources),
            "code_server_identity_id": self._code_server_identity_id,
            "registry_server": self._registry_server,
            "registry_username": self._registry_username,
            "registry_password_secret_name": self._registry_password_secret_name,
        }

    def run_launcher(self) -> AcaRunLauncher:
        launcher = AcaRunLauncher(**self._run_launcher_kwargs())
        launcher.register_instance(self._instance)
        launcher.initialize()
        return launcher
