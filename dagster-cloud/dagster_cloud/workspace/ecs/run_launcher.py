from dagster_aws.ecs import EcsRunLauncher

from .utils import get_task_definition_family


class CloudEcsRunLauncher(EcsRunLauncher):
    def _get_run_task_definition_family(self, run) -> str:
        location_name = (
            run.external_pipeline_origin.external_repository_origin.repository_location_origin.location_name
        )

        return get_task_definition_family(
            "run", self._instance.organization_name, self._instance.deployment_name, location_name
        )
