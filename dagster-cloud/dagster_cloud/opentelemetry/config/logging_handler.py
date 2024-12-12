from dagster import Field, Shape


def logging_handler_schema() -> Shape:
    return Shape(
        description="Provide configuration for the OpenTelemetry logging handler.",
        fields={
            "level": Field(
                str,
                description="The minimum logging level to capture.",
                is_required=False,
            ),
        },
    )
