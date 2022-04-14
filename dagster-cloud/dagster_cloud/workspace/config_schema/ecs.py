from dagster import Array, Field, Noneable, Shape, StringSource

SHARED_ECS_CONFIG = {
    "secrets": Field(
        Noneable(Array(Shape({"name": StringSource, "valueFrom": StringSource}))),
        is_required=False,
        description=(
            "An array of AWS Secrets Manager secrets. These secrets will "
            "be mounted as environment variabls in the container. See "
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
}
