from dagster.core.launcher.base import LaunchRunContext
from dagster.grpc.types import ExecuteRunArgs
from dagster.serdes.serdes import serialize_dagster_namedtuple
from dagster_cloud.execution.utils import TaskStatus
from dagster_cloud.execution.utils.process import check_on_process, kill_process, launch_process

from . import WatchfulRunLauncher

PID_TAG = "process/pid"


class ProcessRunLauncher(WatchfulRunLauncher):
    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.pipeline_run
        pipeline_code_origin = context.pipeline_code_origin

        input_json = serialize_dagster_namedtuple(
            ExecuteRunArgs(
                pipeline_origin=pipeline_code_origin,
                pipeline_run_id=run.run_id,
                instance_ref=self._instance.get_ref(),
            )
        )

        args = ["dagster", "api", "execute_run", input_json]
        pid = launch_process(args)

        self._instance.add_run_tags(run.run_id, {PID_TAG: str(pid)})

    def _get_pid(self, run):
        if not run or run.is_finished:
            return None

        tags = run.tags

        if PID_TAG not in tags:
            return None

        return int(tags[PID_TAG])

    def can_terminate(self, run_id):
        run = self._instance.get_run_by_id(run_id)
        if not run:
            return False

        pid = self._get_pid(run)
        if not pid:
            return False

        return check_on_process(pid) == TaskStatus.RUNNING

    def terminate(self, run_id):
        run = self._instance.get_run_by_id(run_id)
        if not run:
            return False

        pid = self._get_pid(run)
        if not pid:
            return False

        self._instance.report_run_canceling(run)

        kill_process(pid)

        return True

    def check_run_health(self, run_id):
        run = self._instance.get_run_by_id(run_id)
        pid = self._get_pid(run)

        if check_on_process(pid) != TaskStatus.RUNNING:
            self._instance.report_run_failed(run, f"Process pid {pid} is not running")
