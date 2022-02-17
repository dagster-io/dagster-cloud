from dagster_k8s import K8sRunLauncher
from dagster_k8s.job import get_job_name_from_run_id

from . import CloudRunLauncher


class CloudK8sRunLauncher(K8sRunLauncher, CloudRunLauncher):
    def check_run_health(self, run_id):
        job_name = get_job_name_from_run_id(run_id)
        job = self._batch_api.read_namespaced_job(namespace=self.job_namespace, name=job_name)
        pipeline_run = self._instance.get_run_by_id(run_id)
        if job.status.failed:
            self._instance.report_run_failed(
                pipeline_run, f"Kubernetes job {job_name} failed in your cluster."
            )
