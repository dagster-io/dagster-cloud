from __future__ import annotations

import datetime
import logging
import os
import threading
import time
from typing import TYPE_CHECKING, Any

import dagster._check as check
from dagster._core.launcher import RunLauncher
from dagster._core.launcher.base import CheckRunHealthResult, WorkerStatus
from dagster._serdes import ConfigurableClass, ConfigurableClassData

from dagster_cloud.workspace.aca.container_context import AcaContainerContext
from dagster_cloud.workspace.aca.utils import _sanitize_aca_name

if TYPE_CHECKING:
    from azure.mgmt.appcontainers import ContainerAppsAPIClient

logger = logging.getLogger(__name__)

# How long after a run app scales to zero before we delete it.
_DEFAULT_RUN_APP_CLEANUP_MIN_AGE_SECS = 120
# How often the cleanup thread runs.
_CLEANUP_INTERVAL_SECS = 60


class AcaRunLauncher(RunLauncher, ConfigurableClass):
    """Run launcher that executes Dagster runs as Azure Container App instances.

    Each run gets an ephemeral Container App that executes ``dagster api execute_run``
    and is deleted by a background cleanup thread once the run completes.
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        environment_id: str,
        location: str,
        env_vars: list[str] | None = None,
        run_resources: dict[str, Any] | None = None,
        code_server_identity_id: str | None = None,
        registry_server: str | None = None,
        registry_username: str | None = None,
        registry_password_secret_name: str | None = None,
        inst_data: ConfigurableClassData | None = None,
    ):
        self._subscription_id = subscription_id
        self._resource_group = resource_group
        self._environment_id = environment_id
        self._location = location
        self.env_vars = check.opt_list_param(env_vars, "env_vars")
        self._run_resources = check.opt_mapping_param(run_resources, "run_resources")
        self._code_server_identity_id = code_server_identity_id
        self._registry_server = registry_server
        self._registry_username = registry_username
        self._registry_password_secret_name = registry_password_secret_name
        self._inst_data = inst_data

        self._aca_client: ContainerAppsAPIClient | None = None
        self._cleanup_thread: threading.Thread | None = None
        self._cleanup_thread_alive = False

        super().__init__()

    @classmethod
    def config_type(cls):
        # Configured programmatically by AcaUserCodeLauncher.run_launcher() — not from YAML.
        return {}

    @classmethod
    def from_config_value(cls, inst_data: ConfigurableClassData, config_value: dict[str, Any]):
        return cls(inst_data=inst_data, **config_value)

    @property
    def inst_data(self) -> ConfigurableClassData | None:
        return self._inst_data

    def _get_aca_client(self) -> ContainerAppsAPIClient:
        if self._aca_client is None:
            from azure.identity import DefaultAzureCredential
            from azure.mgmt.appcontainers import ContainerAppsAPIClient

            self._aca_client = ContainerAppsAPIClient(
                DefaultAzureCredential(), self._subscription_id
            )
        return self._aca_client

    def _run_app_name(self, run_id: str) -> str:
        return _sanitize_aca_name(f"dagster-run-{run_id[:8]}")

    def initialize(self) -> None:
        self._cleanup_thread_alive = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, name="aca-run-cleanup", daemon=True
        )
        self._cleanup_thread.start()

    def _cleanup_loop(self) -> None:
        while self._cleanup_thread_alive:
            try:
                self._cleanup_completed_run_apps()
            except Exception:
                logger.exception("Error in ACA run app cleanup thread")
            time.sleep(_CLEANUP_INTERVAL_SECS)

    def _cleanup_completed_run_apps(self) -> int:
        """Delete run-worker Container Apps that have finished.

        ACA environments are capped at 200 Container Apps, so completed run
        apps must be removed promptly.
        """
        client = self._get_aca_client()
        min_age = datetime.timedelta(
            seconds=int(os.getenv("RUN_APP_CLEANUP_MIN_AGE_SECS", str(_DEFAULT_RUN_APP_CLEANUP_MIN_AGE_SECS)))
        )
        cutoff = datetime.datetime.now(datetime.timezone.utc) - min_age
        deleted = 0

        try:
            apps = list(client.container_apps.list_by_resource_group(self._resource_group))
        except Exception:
            logger.exception("Failed to list Container Apps for run cleanup")
            return 0

        for app in apps:
            if (app.tags or {}).get("dagster-component") != "run-worker":
                continue
            try:
                running_status = getattr(app.properties, "running_status", None)
                # Only delete apps that are stopped (run completed or failed)
                if running_status not in ("Stopped", None):
                    continue
                # Respect minimum age to avoid deleting apps mid-startup
                system_data = getattr(app, "system_data", None)
                created_at = getattr(system_data, "created_at", None) if system_data else None
                if created_at and created_at > cutoff:
                    continue
                client.container_apps.begin_delete(self._resource_group, app.name)
                logger.info(f"Deleted completed run Container App {app.name!r}")
                deleted += 1
            except Exception:
                logger.exception(f"Failed to delete run Container App {app.name!r}")

        return deleted

    def launch_run(self, context) -> None:
        from azure.mgmt.appcontainers.models import (
            Configuration,
            Container,
            ContainerApp,
            ContainerResources,
            EnvironmentVar,
            ManagedServiceIdentity,
            RegistryCredentials,
            Scale,
            Secret,
            Template,
            UserAssignedIdentity,
        )

        run = context.dagster_run
        run_id = run.run_id
        image = check.not_none(
            run.remote_job_origin.repository_origin.code_location_origin.loadable_target_origin.executable_path
            if hasattr(
                run.remote_job_origin.repository_origin.code_location_origin, "loadable_target_origin"
            )
            else None,
            "Run must have a container image",
        ) if False else context.dagster_run.tags.get("dagster/image")

        # Fall back to the code location's image from run tags
        if not image:
            raise Exception(
                f"No container image found for run {run_id}. "
                "Ensure the code location has an image configured."
            )

        app_name = self._run_app_name(run_id)
        deployment_name = check.not_none(self._instance.deployment_name)

        # Build environment — launcher-level vars + containerContext vars
        container_context = AcaContainerContext(
            env_vars=self.env_vars,
        )

        env_dict = container_context.get_environment_dict()

        # Add Dagster run execution vars
        env_dict.update(
            {
                "DAGSTER_CLOUD_DEPLOYMENT_NAME": deployment_name,
                "DAGSTER_RUN_JOB_NAME": run.job_name,
            }
        )

        env_vars = [EnvironmentVar(name=k, value=v) for k, v in env_dict.items()]

        command = ["dagster", "api", "execute_run"]

        # Resource configuration
        run_resources = self._run_resources
        cpu = float(run_resources.get("cpu", 0.5))
        memory = run_resources.get("memory", "1.0Gi")

        # Registry credentials
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

        identity = None
        if self._code_server_identity_id:
            identity = ManagedServiceIdentity(
                type="UserAssigned",
                user_assigned_identities={self._code_server_identity_id: UserAssignedIdentity()},
            )

        container_app = ContainerApp(
            location=self._location,
            managed_environment_id=self._environment_id,
            identity=identity,
            configuration=Configuration(
                ingress=None,
                secrets=secrets or None,
                registries=registries or None,
                active_revisions_mode="Single",
            ),
            template=Template(
                containers=[
                    Container(
                        name="run-worker",
                        image=image,
                        resources=ContainerResources(cpu=cpu, memory=memory),
                        env=env_vars,
                        command=command,
                    )
                ],
                scale=Scale(
                    # min_replicas=1 ensures ACA doesn't scale-to-zero mid-run
                    min_replicas=1,
                    max_replicas=1,
                    rules=[],
                ),
            ),
            tags={
                "dagster-component": "run-worker",
                "dagster-run-id": run_id,
                "dagster-deployment": deployment_name,
                "managed-by": "dagster-cloud-agent",
            },
        )

        client = self._get_aca_client()
        logger.info(f"Launching run {run_id} as ACA Container App {app_name!r}")
        poller = client.container_apps.begin_create_or_update(
            self._resource_group, app_name, container_app
        )
        poller.result(timeout=180)
        logger.info(f"Launched run {run_id} as {app_name!r}")

    def terminate(self, run_id: str) -> bool:
        app_name = self._run_app_name(run_id)
        try:
            logger.info(f"Terminating run {run_id} by deleting Container App {app_name!r}")
            self._get_aca_client().container_apps.begin_delete(self._resource_group, app_name)
            return True
        except Exception:
            logger.exception(f"Failed to terminate run {run_id}")
            return False

    def check_run_worker_health(self, run) -> CheckRunHealthResult:
        return CheckRunHealthResult(WorkerStatus.UNKNOWN)

    @property
    def supports_check_run_worker_health(self) -> bool:
        return False

    def dispose(self) -> None:
        self._cleanup_thread_alive = False
