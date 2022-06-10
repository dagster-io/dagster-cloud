from dagster import Array, Field, Noneable, Shape, StringSource

SHARED_ECS_CONFIG = {
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
        description="List of environment variable names to include in the ECS task. "
        "Each can be of the form KEY=VALUE or just KEY (in which case the value will be pulled "
        "from the current process)",
    ),
}
