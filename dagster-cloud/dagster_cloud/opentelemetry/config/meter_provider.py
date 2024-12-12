from dagster import Field, Permissive


def meter_provider_config_schema() -> Field:
    return Field(
        config=Permissive(
            description="Provide configuration for an OpenTelemetry meter provider.", fields={}
        )
    )
