import threading
from collections import defaultdict
from typing import Dict, List

import dagster._check as check
from dagster import MetadataEntry, MetadataValue
from dagster.core.events import DagsterEvent, DagsterEventType, EngineEventData
from dagster.core.execution.plan.objects import StepFailureData
from dagster.core.executor.step_delegating import StepHandler
from dagster.core.executor.step_delegating.step_handler.base import StepHandlerContext
from dagster_cloud.execution.utils import TaskStatus
from dagster_cloud.execution.utils.process import check_on_process, kill_process, launch_process


class ProcessStepHandler(StepHandler):
    def __init__(self) -> None:
        super().__init__()
        self._step_pids: Dict[str, Dict[str, int]] = defaultdict(dict)
        self._step_pids_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "ProcessStepHandler"

    def launch_step(self, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:
        step_keys_to_execute = check.not_none(
            step_handler_context.execute_step_args.step_keys_to_execute
        )
        assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        step_key = step_keys_to_execute[0]

        args = step_handler_context.execute_step_args.get_command_args()
        pid = launch_process(args)

        with self._step_pids_lock:
            self._step_pids[step_handler_context.execute_step_args.pipeline_run_id][step_key] = pid
        return []

    def check_step_health(self, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:
        step_keys_to_execute = check.not_none(
            step_handler_context.execute_step_args.step_keys_to_execute
        )
        step_key = step_keys_to_execute[0]

        with self._step_pids_lock:
            pid = self._step_pids[step_handler_context.execute_step_args.pipeline_run_id][step_key]

        status = check_on_process(pid)
        if status == TaskStatus.NOT_FOUND:
            return [
                DagsterEvent(
                    event_type_value=DagsterEventType.STEP_FAILURE.value,
                    pipeline_name=step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                    step_key=step_key,
                    message=f"Process PID {pid} for step {step_key} is not running",
                    event_specific_data=StepFailureData(
                        error=None,
                        user_failure_data=None,
                    ),
                )
            ]
        else:
            return []

    def terminate_step(self, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:
        step_keys_to_execute = check.not_none(
            step_handler_context.execute_step_args.step_keys_to_execute
        )
        assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        step_key = step_keys_to_execute[0]

        with self._step_pids_lock:
            pid = self._step_pids[step_handler_context.execute_step_args.pipeline_run_id][step_key]

        events = [
            DagsterEvent(
                event_type_value=DagsterEventType.ENGINE_EVENT.value,
                pipeline_name=step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                step_key=step_key,
                message="Stopping process for step",
                event_specific_data=EngineEventData(
                    [
                        MetadataEntry("Step key", value=MetadataValue.text(step_key)),
                        MetadataEntry("Process id", value=MetadataValue.int(pid)),
                    ],
                ),
            )
        ]
        kill_process(pid)
        return events
