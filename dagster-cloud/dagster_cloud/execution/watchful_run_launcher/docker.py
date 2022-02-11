from dagster_cloud.execution.utils import TaskStatus
from dagster_cloud.execution.utils.docker import check_on_container
from dagster_cloud.execution.watchful_run_launcher.base import WatchfulRunLauncher
from dagster_docker import DockerRunLauncher
from dagster_docker.docker_run_launcher import DOCKER_CONTAINER_ID_TAG


class WatchfulDockerRunLauncher(DockerRunLauncher, WatchfulRunLauncher):
    def check_run_health(self, run_id):
        run = self._instance.get_run_by_id(run_id)
        container_id = run.tags.get(DOCKER_CONTAINER_ID_TAG)
        status, msg = check_on_container(self._get_client(), container_id)

        if status not in [TaskStatus.RUNNING, TaskStatus.SUCCESS]:
            self._instance.report_run_failed(run, f"Container {container_id} is not running: {msg}")
