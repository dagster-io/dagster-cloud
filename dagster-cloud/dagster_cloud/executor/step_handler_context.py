from typing import Dict, NamedTuple

from dagster import DagsterInstance, check
from dagster.core.executor.step_delegating import StepHandlerContext
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.grpc.types import ExecuteStepArgs
from dagster.serdes.serdes import whitelist_for_serdes


@whitelist_for_serdes
class PersistedDagsterCloudStepHandlerContext(
    NamedTuple(
        "_PersistedDagsterCloudStepHandlerContext",
        [
            ("execute_step_args", ExecuteStepArgs),
            ("step_tags", Dict[str, Dict[str, str]]),
            ("pipeline_run", PipelineRun),
        ],
    )
):
    def __new__(
        cls,
        execute_step_args: ExecuteStepArgs,
        step_tags: Dict[str, Dict[str, str]],
        pipeline_run: PipelineRun,
    ):
        return super(PersistedDagsterCloudStepHandlerContext, cls).__new__(
            cls,
            check.inst_param(execute_step_args, "execute_step_args", ExecuteStepArgs),
            check.dict_param(step_tags, "step_tags", key_type=str, value_type=Dict),
            check.inst_param(pipeline_run, "pipeline_run", PipelineRun),
        )


class DagsterCloudStepHandlerContext(StepHandlerContext):
    def serialize_for_cloud_api(self) -> PersistedDagsterCloudStepHandlerContext:
        # serialized contexts are passed to the user cloud, and should not contain host instances
        self._execute_step_args: ExecuteStepArgs = self._execute_step_args._replace(
            instance_ref=None
        )
        return PersistedDagsterCloudStepHandlerContext(
            execute_step_args=self.execute_step_args,
            step_tags=self.step_tags,
            pipeline_run=self.pipeline_run,
        )

    @classmethod
    def deserialize(
        cls, instance: DagsterInstance, ctx_tuple: PersistedDagsterCloudStepHandlerContext
    ):
        execute_step_args = ctx_tuple.execute_step_args._replace(instance_ref=instance.get_ref())
        return cls(
            instance=instance,
            execute_step_args=execute_step_args,
            step_tags=ctx_tuple.step_tags,
            pipeline_run=ctx_tuple.pipeline_run,
        )

    @classmethod
    def from_oss_context(cls, context: StepHandlerContext):
        ctx = cls(
            instance=context.instance,
            execute_step_args=context.execute_step_args,
            step_tags=context.step_tags,
            pipeline_run=context.pipeline_run,
        )
        return ctx
