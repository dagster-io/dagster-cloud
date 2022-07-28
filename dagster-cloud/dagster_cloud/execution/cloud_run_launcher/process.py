import time

import dagster._check as check
from dagster._core.launcher import RunLauncher
from dagster._core.launcher.base import LaunchRunContext
from dagster._grpc.types import ExecuteRunArgs
from dagster_cloud.execution.utils import TaskStatus
from dagster_cloud.execution.utils.process import check_on_process, kill_process, launch_process

PID_TAG = "process/pid"


class CloudProcessRunLauncher(RunLauncher):
    def __init__(self):
        self._run_ids = set()
        super().__init__()

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.pipeline_run
        pipeline_code_origin = check.not_none(context.pipeline_code_origin)

        run_args = ExecuteRunArgs(
            pipeline_origin=pipeline_code_origin,
            pipeline_run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        )

        args = run_args.get_command_args()
        pid = launch_process(args)

        self._run_ids.add(run.run_id)

        self._instance.add_run_tags(run.run_id, {PID_TAG: str(pid)})

    def join(self, timeout=30):
        total_time = 0
        interval = 0.01

        while True:
            active_run_ids = [
                run_id
                for run_id in self._run_ids
                if (
                    self._instance.get_run_by_id(run_id)
                    and not self._instance.get_run_by_id(run_id).is_finished
                )
            ]

            if len(active_run_ids) == 0:
                return

            if total_time >= timeout:
                raise Exception(
                    "Timed out waiting for these runs to finish: {active_run_ids}".format(
                        active_run_ids=repr(active_run_ids)
                    )
                )

            total_time += interval
            time.sleep(interval)
            interval = interval * 2

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
