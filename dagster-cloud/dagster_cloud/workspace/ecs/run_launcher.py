import dagster._check as check
from dagster_aws.ecs import EcsRunLauncher

from dagster_cloud.instance import DagsterCloudAgentInstance

from .utils import get_task_definition_family


class CloudEcsRunLauncher(EcsRunLauncher[DagsterCloudAgentInstance]):
    def _get_run_task_definition_family(self, run) -> str:
        pipeline_origin = check.not_none(run.external_pipeline_origin)
        location_name = (
            pipeline_origin.external_repository_origin.code_location_origin.location_name
        )

        return get_task_definition_family(
            "run", self._instance.organization_name, self._instance.deployment_name, location_name  # type: ignore  # (possible none)
        )
