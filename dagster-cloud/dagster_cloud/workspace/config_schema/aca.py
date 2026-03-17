from dagster import Array, Field, Permissive, StringSource

# Shared between user code launcher config and container_context
SHARED_ACA_CONFIG = {
    "server_resources": Field(
        Permissive(
            {
                "cpu": Field(
                    str,
                    is_required=False,
                    description="CPU to allocate to code server containers (e.g. '0.5', '1.0', '2.0').",
                ),
                "memory": Field(
                    str,
                    is_required=False,
                    description="Memory to allocate to code server containers (e.g. '1.0Gi', '2Gi').",
                ),
            }
        ),
        is_required=False,
        description="Resource overrides for code server containers.",
    ),
    "run_resources": Field(
        Permissive(
            {
                "cpu": Field(
                    str,
                    is_required=False,
                    description="CPU to allocate to run worker containers (e.g. '0.5', '1.0', '2.0').",
                ),
                "memory": Field(
                    str,
                    is_required=False,
                    description="Memory to allocate to run worker containers (e.g. '1.0Gi', '2Gi').",
                ),
            }
        ),
        is_required=False,
        description="Resource overrides for run worker containers.",
    ),
}


ACA_CONTAINER_CONTEXT_CONFIG = {
    "env_vars": Field(
        Array(StringSource),
        is_required=False,
        description=(
            "List of environment variable names to include in the ACA container. "
            "Each can be of the form KEY=VALUE or just KEY (in which case the value will be pulled "
            "from the current process)."
        ),
    ),
    **SHARED_ACA_CONFIG,
}
