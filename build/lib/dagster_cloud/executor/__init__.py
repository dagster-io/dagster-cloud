from typing import Any, Dict

from dagster import default_executors, executor
from dagster.config.config_type import Array, Noneable
from dagster.config.field import Field
from dagster.config.field_utils import Permissive, Shape
from dagster.config.source import BoolSource, StringSource
from dagster.core.definitions.executor_definition import multiple_process_executor_requirements
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.execution.retries import get_retries_config
from dagster.utils.merger import merge_dicts

DAGSTER_CLOUD_EXECUTOR_NAME = "cloud_isolated_step"

DAGSTER_CLOUD_EXECUTOR_K8S_CONFIG_KEY = "k8s"

# to avoid a dagster-k8s dependency, we replicate the config schema here, and test that it matches.
DAGSTER_CLOUD_K8S_EXECUTOR_CONFIG_SCHEMA = {
    "job_image": Field(
        Noneable(StringSource),
        is_required=False,
        description="Docker image to use for launched task Jobs. If the repository is not "
        "loaded from a GRPC server, then this field is required. If the repository is "
        "loaded from a GRPC server, then leave this field empty."
        '(Ex: "mycompany.com/dagster-k8s-image:latest").',
    ),
    "image_pull_policy": Field(
        Noneable(StringSource),
        is_required=False,
        description="Image pull policy to set on the launched task Job Pods. Defaults to "
        '"IfNotPresent".',
        default_value=None,
    ),
    "image_pull_secrets": Field(
        Noneable(Array(Shape({"name": StringSource}))),
        is_required=False,
        description="(Advanced) Specifies that Kubernetes should get the credentials from "
        "the Secrets named in this list.",
    ),
    "service_account_name": Field(
        Noneable(StringSource),
        is_required=False,
        description="(Advanced) Override the name of the Kubernetes service account under "
        "which to run the Job.",
    ),
    "env_config_maps": Field(
        Noneable(Array(StringSource)),
        is_required=False,
        description="A list of custom ConfigMapEnvSource names from which to draw "
        "environment variables (using ``envFrom``) for the Job. Default: ``[]``. See:"
        "https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container/#define-an-environment-variable-for-a-container",
    ),
    "env_secrets": Field(
        Noneable(Array(StringSource)),
        is_required=False,
        description="A list of custom Secret names from which to draw environment "
        "variables (using ``envFrom``) for the Job. Default: ``[]``. See:"
        "https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure/#configure-all-key-value-pairs-in-a-secret-as-container-environment-variables",
    ),
    "env_vars": Field(
        Noneable(Array(str)),
        is_required=False,
        description="A list of environment variables to inject into the Job. "
        "Default: ``[]``. See: "
        "https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure/#configure-all-key-value-pairs-in-a-secret-as-container-environment-variables",
    ),
    "volume_mounts": Field(
        Array(
            Shape(
                {
                    "name": StringSource,
                    "mountPath": StringSource,
                    "mountPropagation": Field(StringSource, is_required=False),
                    "readOnly": Field(BoolSource, is_required=False),
                    "subPath": Field(StringSource, is_required=False),
                    "subPathExpr": Field(StringSource, is_required=False),
                }
            )
        ),
        is_required=False,
        default_value=[],
        description="A list of volume mounts to include in the job's container. Default: ``[]``. See: "
        "https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volumemount-v1-core",
    ),
    "volumes": Field(
        Array(
            Permissive(
                {
                    "name": str,
                }
            )
        ),
        is_required=False,
        default_value=[],
        description="A list of volumes to include in the Job's Pod. Default: ``[]``. For the many "
        "possible volume source types that can be included, see: "
        "https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volume-v1-core",
    ),
}


@executor(
    name=DAGSTER_CLOUD_EXECUTOR_NAME,
    config_schema=Permissive(
        fields={
            DAGSTER_CLOUD_EXECUTOR_K8S_CONFIG_KEY: Field(
                merge_dicts(
                    DAGSTER_CLOUD_K8S_EXECUTOR_CONFIG_SCHEMA,
                    {"job_namespace": Field(StringSource, is_required=False)},
                ),
                is_required=False,
            ),
            "retries": get_retries_config(),
        }
    ),
    requirements=multiple_process_executor_requirements(),
)
def cloud_isolated_step_executor(_context):
    raise DagsterInvariantViolationError("This executor should never run within user code")


default_cloud_executors = default_executors + [cloud_isolated_step_executor]
