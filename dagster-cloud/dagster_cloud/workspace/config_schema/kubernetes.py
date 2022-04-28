from dagster import Array, BoolSource, Field, Noneable, Permissive, Shape, StringSource

SHARED_K8S_CONFIG = {
    "namespace": Field(
        Noneable(StringSource),
        is_required=False,
        description="The namespace into which to launch Kubernetes resources. Note that any "
        "other required resources (such as the service account) must be "
        "present in this namespace.",
    ),
    "image_pull_policy": Field(
        Noneable(StringSource),
        is_required=False,
        description="Image pull policy to set on launched Pods.",
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
    "service_account_name": Field(
        Noneable(StringSource),
        is_required=False,
        description="The name of the Kubernetes service account under which to run.",
    ),
    "volume_mounts": Field(
        Array(
            # Can supply either snake_case or camelCase, but in typeaheads based on the
            # schema we assume snake_case
            Permissive(
                {
                    "name": StringSource,
                    "mount_path": Field(StringSource, is_required=False),
                    "mount_propagation": Field(StringSource, is_required=False),
                    "read_only": Field(BoolSource, is_required=False),
                    "sub_path": Field(StringSource, is_required=False),
                    "sub_path_expr": Field(StringSource, is_required=False),
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
    "image_pull_secrets": Field(
        Noneable(Array(Shape({"name": StringSource}))),
        is_required=False,
        description="Specifies that Kubernetes should get the credentials from "
        "the Secrets named in this list.",
    ),
    "labels": Field(
        dict,
        is_required=False,
        description="Labels to apply to all created pods. See: "
        "https://kubernetes.io/docs/concepts/overview/working-with-objects/labels",
    ),
    "resources": Field(
        Noneable(
            {
                "limits": Field(dict, is_required=False),
                "requests": Field(dict, is_required=False),
            }
        ),
        is_required=False,
        description="Compute resource requirements for the container. See: "
        "https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/",
    ),
}
