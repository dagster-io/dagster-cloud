from dagster import Array, Field, Noneable, Permissive, Shape, StringSource

# Shared between user code launcher config and container context
SHARED_ECS_CONFIG = {
    "server_resources": Field(
        Permissive(
            {
                "cpu": Field(
                    str,
                    is_required=False,
                    description="The CPU override to use for the launched task.",
                ),
                "memory": Field(
                    str,
                    is_required=False,
                    description="The memory override to use for the launched task.",
                ),
            }
        )
    ),
    "run_resources": Field(
        Permissive(
            {
                "cpu": Field(
                    str,
                    is_required=False,
                    description="The CPU override to use for the launched task.",
                ),
                "memory": Field(
                    str,
                    is_required=False,
                    description="The memory override to use for the launched task.",
                ),
            }
        )
    ),
    "runtime_platform": Field(
        Shape(
            {
                "cpuArchitecture": Field(StringSource, is_required=False),
                "operatingSystemFamily": Field(StringSource, is_required=False),
            }
        ),
        is_required=False,
        description=(
            "The operating system that the task definition is running on. See"
            " https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.register_task_definition"
            " for the available options."
        ),
    ),
}


ECS_CONTAINER_CONTEXT_CONFIG = {
    "secrets": Field(
        Noneable(Array(Shape({"name": StringSource, "valueFrom": StringSource}))),
        is_required=False,
        description=(
            "An array of AWS Secrets Manager secrets. These secrets will "
            "be mounted as environment variables in the container. See "
            "https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_Secret.html."
        ),
    ),
    "secrets_tags": Field(
        Noneable(Array(StringSource)),
        is_required=False,
        description=(
            "AWS Secrets Manager secrets with these tags will be mounted as "
            "environment variables in the container."
        ),
    ),
    "env_vars": Field(
        [StringSource],
        is_required=False,
        description=(
            "List of environment variable names to include in the ECS task. "
            "Each can be of the form KEY=VALUE or just KEY (in which case the value will be pulled "
            "from the current process)"
        ),
    ),
    "task_role_arn": Field(
        StringSource,
        is_required=False,
        description=(
            "ARN of the IAM role for launched tasks. See"
            " https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html. "
        ),
    ),
    "execution_role_arn": Field(
        StringSource,
        is_required=False,
        description=(
            "ARN of the task execution role for the ECS container and Fargate agent to make AWS API"
            " calls on your behalf. See"
            " https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_execution_IAM_role.html. "
        ),
    ),
    **SHARED_ECS_CONFIG,
}
