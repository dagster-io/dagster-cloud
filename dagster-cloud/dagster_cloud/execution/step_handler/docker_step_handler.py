import os
from typing import List

import docker
from dagster import MetadataEntry, MetadataValue
from dagster import _check as check
from dagster.core.events import DagsterEvent, DagsterEventType, EngineEventData
from dagster.core.execution.plan.objects import StepFailureData
from dagster.core.executor.step_delegating.step_handler.base import StepHandler, StepHandlerContext
from dagster.serdes.utils import hash_str
from dagster_cloud.execution.utils import TaskStatus
from dagster_cloud.execution.utils.docker import check_on_container


class DockerStepHandler(StepHandler):
    def __init__(
        self,
        networks,
        env_vars,
        container_kwargs=None,
    ):
        super().__init__()
        self._networks = check.opt_list_param(networks, "networks", of_type=str)
        self._env_vars = check.opt_list_param(env_vars, "env_vars", of_type=str)
        self._container_kwargs = check.opt_dict_param(
            container_kwargs, "container_kwargs", key_type=str
        )

    @property
    def name(self) -> str:
        return "DockerStepHandler"

    def _get_container_name(self, run_id, step_key):
        return f"dagster-job-{hash_str(run_id + step_key)}"

    def _create_step_container(self, client, step_image, execute_step_args):

        return client.containers.create(
            step_image,
            name=self._get_container_name(
                execute_step_args.pipeline_run_id, execute_step_args.step_keys_to_execute[0]
            ),
            detach=True,
            network=self._networks[0] if len(self._networks) else None,
            command=execute_step_args.get_command_args(),
            environment=(
                {env_name: os.getenv(env_name) for env_name in self._env_vars}
                if self._env_vars
                else {}
            ),
            labels={
                "created_by": "DagsterStepHandler",
                "step_key": execute_step_args.step_keys_to_execute[0],
                "run_id": execute_step_args.pipeline_run_id,
            },
            **self._container_kwargs,
        )

    def launch_step(self, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:
        client = docker.client.from_env()

        step_image = (
            step_handler_context.execute_step_args.pipeline_origin.repository_origin.container_image
        )

        if not step_image:
            raise Exception("No image included to launch steps: " + str(step_image))

        try:
            step_container = self._create_step_container(
                client, step_image, step_handler_context.execute_step_args
            )
        except docker.errors.ImageNotFound:
            client.images.pull(step_image)
            step_container = self._create_step_container(
                client, step_image, step_handler_context.execute_step_args
            )

        if len(self._networks) > 1:
            for network_name in self._networks[1:]:
                network = client.networks.get(network_name)
                network.connect(step_container)

        step_keys_to_execute = check.is_list(
            step_handler_context.execute_step_args.step_keys_to_execute
        )
        assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        step_key = step_keys_to_execute[0]

        events = [
            DagsterEvent(
                event_type_value=DagsterEventType.ENGINE_EVENT.value,
                pipeline_name=step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                step_key=step_key,
                message="Launching step in Docker container",
                event_specific_data=EngineEventData(
                    [
                        MetadataEntry("Step key", value=MetadataValue.text(step_key)),
                        MetadataEntry(
                            "Docker container id", value=MetadataValue.text(step_container.id)
                        ),
                    ],
                ),
            )
        ]

        step_container.start()

        return events

    def check_step_health(self, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:
        step_keys_to_execute = check.is_list(
            step_handler_context.execute_step_args.step_keys_to_execute
        )
        step_key = step_keys_to_execute[0]

        client = docker.client.from_env()

        container_name = self._get_container_name(
            step_handler_context.execute_step_args.pipeline_run_id,
            step_key,
        )

        status, msg = check_on_container(client, container_name)
        if status not in [TaskStatus.RUNNING, TaskStatus.SUCCESS]:
            return [
                DagsterEvent(
                    event_type_value=DagsterEventType.STEP_FAILURE.value,
                    pipeline_name=step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                    step_key=step_key,
                    message=f"Container {container_name} for step {step_key} is not running: {msg}",
                    event_specific_data=StepFailureData(
                        error=None,
                        user_failure_data=None,
                    ),
                )
            ]
        else:
            return []

    def terminate_step(self, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:

        step_keys_to_execute = check.is_list(
            step_handler_context.execute_step_args.step_keys_to_execute
        )
        assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        step_key = step_keys_to_execute[0]

        events = [
            DagsterEvent(
                event_type_value=DagsterEventType.ENGINE_EVENT.value,
                pipeline_name=step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                step_key=step_key,
                message="Stopping Docker container for step",
                event_specific_data=EngineEventData(),
            )
        ]

        client = docker.client.from_env()

        try:
            container = client.containers.get(
                self._get_container_name(
                    step_handler_context.execute_step_args.pipeline_run_id,
                    step_key,
                )
            )
            container.stop()
        except Exception as e:
            events.append(
                DagsterEvent(
                    event_type_value=DagsterEventType.ENGINE_EVENT.value,
                    pipeline_name=step_handler_context.execute_step_args.pipeline_origin.pipeline_name,
                    step_key=step_key,
                    message=f"Hit error while terminating Docker container:\n{e}",
                    event_specific_data=EngineEventData(),
                )
            )

        return events
