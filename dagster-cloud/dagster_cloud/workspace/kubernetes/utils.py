import copy
import re
import time

import kubernetes
from dagster._utils.merger import merge_dicts
from dagster_k8s.models import k8s_model_from_dict
from kubernetes import client

from ..user_code_launcher.utils import (
    deterministic_label_for_location,
    get_human_readable_label,
    unique_resource_name,
)

MANAGED_RESOURCES_LABEL = {"managed_by": "K8sUserCodeLauncher"}
SERVICE_PORT = 4000


def _sanitize_k8s_resource_name(name):
    filtered_name = re.sub("[^a-z0-9-]", "", name.lower())

    # ensure it doesn't start with a non-alpha character
    while filtered_name and re.match("[^a-z].*", filtered_name):
        filtered_name = filtered_name[1:]

    filtered_name = filtered_name.strip("-")

    # always return something that starts with a letter in the unlikely event that everything is
    # filtered out (doesn't have to be unique)
    return filtered_name or "k8s"


def unique_k8s_resource_name(deployment_name, location_name):
    """
    https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names

    K8s resource names are restricted, so we must sanitize the location name to not include disallowed characters.
    """
    return unique_resource_name(
        deployment_name, location_name, length_limit=63, sanitize_fn=_sanitize_k8s_resource_name
    )


def get_k8s_human_readable_label(name):
    """
    https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

    K8s label values are restricted, so we must sanitize the location name to not include disallowed characters.
    These are purely to help humans debug, so they don't need to be unique.
    """
    return get_human_readable_label(
        name,
        length_limit=63,
        sanitize_fn=lambda name: (
            re.sub("[^a-zA-Z0-9-_.]", "", name).strip("-").strip("_").strip(".")
        ),
    )


def construct_repo_location_service(deployment_name, location_name, service_name):
    return client.V1Service(
        metadata=client.V1ObjectMeta(
            name=service_name,
            labels={
                **MANAGED_RESOURCES_LABEL,
                "location_hash": deterministic_label_for_location(deployment_name, location_name),
                "location_name": get_k8s_human_readable_label(location_name),
                "deployment_name": get_k8s_human_readable_label(deployment_name),
            },
        ),
        spec=client.V1ServiceSpec(
            selector={"user-deployment": service_name},
            ports=[client.V1ServicePort(name="http", protocol="TCP", port=SERVICE_PORT)],
        ),
    )


def construct_repo_location_deployment(
    instance,
    deployment_name,
    location_name,
    k8s_deployment_name,
    metadata,
    container_context,
    args,
):
    pull_policy = container_context.image_pull_policy
    env_config_maps = container_context.env_config_maps
    env_secrets = container_context.env_secrets
    service_account_name = container_context.service_account_name
    image_pull_secrets = container_context.image_pull_secrets
    volume_mounts = container_context.volume_mounts

    volumes = container_context.volumes
    labels = container_context.labels
    resources = container_context.resources

    scheduler_name = container_context.scheduler_name
    security_context = container_context.security_context

    env = merge_dicts(
        metadata.get_grpc_server_env(
            SERVICE_PORT, location_name, instance.ref_for_deployment(deployment_name)
        ),
        container_context.get_environment_dict(),
    )

    user_defined_config = container_context.get_server_user_defined_k8s_config()

    container_config = copy.deepcopy(user_defined_config.container_config)

    container_config["args"] = args

    if pull_policy:
        container_config["image_pull_policy"] = pull_policy

    user_defined_env_vars = container_config.pop("env", [])
    user_defined_env_from = container_config.pop("env_from", [])
    user_defined_volume_mounts = container_config.pop("volume_mounts", [])
    user_defined_resources = container_config.pop("resources", {})
    user_defined_security_context = container_config.pop("security_context", None)

    container_config = {
        **container_config,
        "name": "dagster",
        "image": metadata.image,
        "env": [{"name": key, "value": value} for key, value in env.items()]
        + user_defined_env_vars,
        "env_from": [{"config_map_ref": {"name": config_map}} for config_map in env_config_maps]
        + [{"secret_ref": {"name": secret_name}} for secret_name in env_secrets]
        + user_defined_env_from,
        "volume_mounts": volume_mounts + user_defined_volume_mounts,
        "resources": user_defined_resources or resources,
        "security_context": user_defined_security_context or security_context,
    }

    pod_spec_config = copy.deepcopy(user_defined_config.pod_spec_config)

    user_defined_image_pull_secrets = pod_spec_config.pop("image_pull_secrets", [])
    user_defined_service_account_name = pod_spec_config.pop("service_account_name", None)
    user_defined_containers = pod_spec_config.pop("containers", [])
    user_defined_volumes = pod_spec_config.pop("volumes", [])
    user_defined_scheduler_name = pod_spec_config.pop("scheduler_name", None)

    pod_spec_config = {
        **pod_spec_config,
        "image_pull_secrets": (
            [{"name": x["name"]} for x in image_pull_secrets] + user_defined_image_pull_secrets
        ),
        "service_account_name": user_defined_service_account_name or service_account_name,
        "containers": [container_config] + user_defined_containers,
        "volumes": volumes + user_defined_volumes,
        "scheduler_name": user_defined_scheduler_name or scheduler_name,
    }

    pod_template_spec_metadata = copy.deepcopy(user_defined_config.pod_template_spec_metadata)
    user_defined_pod_template_labels = pod_template_spec_metadata.pop("labels", {})

    deployment_dict = {
        "metadata": {
            "name": k8s_deployment_name,
            "labels": {
                **MANAGED_RESOURCES_LABEL,
                "location_hash": deterministic_label_for_location(deployment_name, location_name),
                "location_name": get_k8s_human_readable_label(location_name),
                "deployment_name": get_k8s_human_readable_label(deployment_name),
            },
        },
        "spec": {  # DeploymentSpec
            "selector": {"match_labels": {"user-deployment": k8s_deployment_name}},
            "template": {  # PodTemplateSpec
                "metadata": {
                    **pod_template_spec_metadata,
                    "labels": {
                        "user-deployment": k8s_deployment_name,
                        **labels,
                        **user_defined_pod_template_labels,
                    },
                },
                "spec": pod_spec_config,
            },
        },
    }

    return k8s_model_from_dict(
        kubernetes.client.V1Deployment,
        deployment_dict,
    )


def did_pod_image_fail(pod):
    if (not pod.status.container_statuses) or len(pod.status.container_statuses) == 0:
        return False

    container_waiting_state = pod.status.container_statuses[0].state.waiting
    if not container_waiting_state:
        return False

    waiting_reason = container_waiting_state.reason

    return waiting_reason == "ImagePullBackOff" or waiting_reason == "ErrImageNeverPull"


def wait_for_deployment_complete(
    k8s_deployment_name,
    namespace,
    logger,
    location_name,
    metadata,
    existing_pods,
    timeout,
    image_pull_grace_period,
):
    """
    Translated from
    https://github.com/kubernetes/kubectl/blob/ac49920c0ccb0dd0899d5300fc43713ee2dfcdc9/pkg/polymorphichelpers/rollout_status.go#L75-L91
    """
    api = client.AppsV1Api(client.ApiClient())
    core_api = client.CoreV1Api()

    existing_pod_names = (pod.metadata.name for pod in existing_pods)

    start = time.time()
    while True:
        time.sleep(2)

        time_elapsed = time.time() - start

        if time_elapsed >= timeout:
            raise Exception(f"Timed out waiting for deployment {k8s_deployment_name}")

        deployment = api.read_namespaced_deployment(k8s_deployment_name, namespace)
        status = deployment.status
        spec = deployment.spec

        logger.debug(
            f"[updated_replicas:{status.updated_replicas},replicas:{status.replicas},available_replicas:{status.available_replicas},observed_generation:{status.observed_generation}]"
            " waiting..."
        )
        logger.debug(f"Status: {status}, spec: {spec}")

        if (
            status.updated_replicas == spec.replicas  # new replicas have been updated
            and status.replicas == status.updated_replicas  # no old replicas pending termination
            and status.available_replicas == status.updated_replicas  # updated replicas available
            and status.observed_generation >= deployment.metadata.generation  # new spec observed
        ):
            return True

        pod_list = core_api.list_namespaced_pod(
            namespace, label_selector="user-deployment={}".format(k8s_deployment_name)
        )

        if time_elapsed >= image_pull_grace_period:
            for pod in pod_list.items:
                if pod.metadata.name not in existing_pod_names and did_pod_image_fail(pod):
                    raise Exception(
                        f"Failed to pull image {metadata.image} for location {location_name}"
                    )
