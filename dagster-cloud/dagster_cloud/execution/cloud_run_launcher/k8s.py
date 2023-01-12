import sys

from dagster._core.launcher.base import CheckRunHealthResult, WorkerStatus
from dagster._core.storage.pipeline_run import DagsterRun
from dagster._utils.error import serializable_error_info_from_exc_info
from dagster_k8s import K8sRunLauncher
from dagster_k8s.job import get_job_name_from_run_id


class CloudK8sRunLauncher(K8sRunLauncher):
    # Fork to avoid call to `count_resume_run_attempts`, since resuming runs is not currently
    # supported in cloud and the method makes repeated event log calls.
    def check_run_worker_health(self, run: DagsterRun):
        container_context = self.get_container_context_for_run(run)

        job_name = get_job_name_from_run_id(
            run.run_id,
        )
        try:
            status = self._api_client.get_job_status(
                namespace=container_context.namespace, job_name=job_name
            )
        except Exception:
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN,
                str(serializable_error_info_from_exc_info(sys.exc_info())),
            )
        if status.failed:
            return CheckRunHealthResult(WorkerStatus.FAILED, "K8s job failed")
        if status.succeeded:
            return CheckRunHealthResult(WorkerStatus.SUCCESS)
        return CheckRunHealthResult(WorkerStatus.RUNNING)
