from typing import Any, Dict, List, Optional, cast

from dagster import Noneable
from dagster import _check as check
from dagster.config import Field, Map, Selector, Shape
from dagster.config.errors import EvaluationError
from dagster.config.source import StringSource
from dagster.config.validate import validate_config
from dagster.utils import frozendict

from .docker import SHARED_DOCKER_CONFIG
from .ecs import SHARED_ECS_CONFIG
from .kubernetes import SHARED_K8S_CONFIG


def validate_workspace_location(workspace_location) -> Optional[List[str]]:
    """Processes a single workspace location config. Returns a list of error
    messages if any."""
    validation = validate_config(LOCATION_CONFIG_SCHEMA, workspace_location)
    return [error.message for error in validation.errors or []]


def validate_workspace_config(workspace_config) -> Optional[List[str]]:
    """Processes an entire workspace location config. Returns a list of
    error messages, if any."""
    validation = validate_config(WORKSPACE_CONFIG_SCHEMA, workspace_config)
    return [error.message for error in validation.errors or []]


def process_workspace_config(workspace_config) -> Dict[str, Any]:
    """Checks a workspace config, erroring if any mismatches with config
    and migrating an input in the legacy workspace config format to the
    modern format, returning the validated input."""
    check.dict_param(workspace_config, "workspace_config")

    # Check if using the legacy format, see below for details
    if isinstance(workspace_config.get("locations"), (dict, frozendict)):
        check.is_dict(workspace_config.get("locations"))

        validation = validate_config(LEGACY_WORKSPACE_CONFIG_SCHEMA, workspace_config)
        check.invariant(
            validation.success,
            ", ".join([error.message for error in validation.errors or []]),
        )

        locations = workspace_config["locations"].values()

        # Manually validate that only one of python_file, package_name, module_name specified
        # modern format will check this automatically as it uses a Selector ConfigType
        for config in locations:
            python_file = config.get("python_file")
            package_name = config.get("package_name")
            module_name = config.get("module_name")
            check.invariant(
                len([val for val in [python_file, package_name, module_name] if val]) == 1,
                "Must supply exactly one of a file name, a package name, or a module name",
            )

        # Convert legacy formatted locations to modern format
        updated_locations = []
        for name, location in workspace_config["locations"].items():
            new_location = {
                k: v
                for k, v in location.items()
                if k not in ("python_file", "package_name", "module_name")
            }
            new_location["code_source"] = {}
            if "python_file" in location:
                new_location["code_source"]["python_file"] = location["python_file"]
            elif "package_name" in location:
                new_location["code_source"]["package_name"] = location["package_name"]
            elif "module_name" in location:
                new_location["code_source"]["module_name"] = location["module_name"]

            new_location["location_name"] = name
            updated_locations.append(new_location)
        return {"locations": updated_locations}
    else:
        check.is_list(workspace_config.get("locations"))

        validation = validate_config(WORKSPACE_CONFIG_SCHEMA, workspace_config)
        check.invariant(
            validation.success,
            ", ".join([error.message for error in cast(List[EvaluationError], validation.errors)]),
        )
        return workspace_config


CONFIG_SCHEMA_FIELDS = {
    # Only used by the new workspace.yaml format, legacy format has the name as a key
    "location_name": Field(config=str, is_required=True, description="Location name"),
    "image": Field(
        config=Noneable(str),
        is_required=False,
        description="Docker image, for use with containerized agents.",
    ),
    "code_source": Field(
        Selector(
            {
                "python_file": Field(
                    config=str,
                    description="Python file containing the target Dagster repository.",
                ),
                "package_name": Field(
                    config=str,
                    description="Python package containing the target Dagster repository.",
                ),
                "module_name": Field(
                    config=str,
                    description="Python module containing the target Dagster repository.",
                ),
            },
        ),
        description="Python entry point for the code location.",
    ),
    "working_directory": Field(
        config=str,
        is_required=False,
        description="Working directory to use for importing Python modules when loading the repository.",
    ),
    "executable_path": Field(
        config=str,
        is_required=False,
        description="Path to reach the executable to use for the Python environment to load the repository. Defaults to the installed dagster command-line entry point.",
    ),
    "attribute": Field(
        config=str,
        is_required=False,
        description="Specifies either a repository or a function that returns a repository. Can be used when the code contains multiple repositories but only one should be included.",
    ),
    "git": Field(
        Shape(
            fields={
                "commit_hash": Field(
                    config=str,
                    is_required=False,
                    description="Indicates the commit sha associated with this location.",
                ),
                "url": Field(
                    config=str,
                    is_required=False,
                    description="Specifies a source code reference link for this location.",
                ),
            }
        ),
        is_required=False,
    ),
    "container_context": Field(
        Shape(
            fields={
                "k8s": Field(Shape(SHARED_K8S_CONFIG), is_required=False),
                "docker": Field(Shape(SHARED_DOCKER_CONFIG), is_required=False),
                "ecs": Field(Shape(SHARED_ECS_CONFIG), is_required=False),
            },
        ),
        description="Metadata for specific compute environments",
        is_required=False,
    ),
}


# The legacy format is structured as
# locations:
#   my_location_name:
#     image: abc
#     python_file: repo.py
#
# Here, we drop the fields not included in the legacy format, including location name (which is the
# dict key in the legacy format) and the code_source field (whose children are top-level values).
LEGACY_CONFIG_SCHEMA_FIELDS = {
    **{k: v for k, v in CONFIG_SCHEMA_FIELDS.items() if k not in ("location_name", "code_source")},
    "python_file": Field(
        config=StringSource,
        is_required=False,
        description="Python file containing the target Dagster repository.",
    ),
    "package_name": Field(
        config=StringSource,
        is_required=False,
        description="Python package containing the target Dagster repository.",
    ),
    "module_name": Field(
        config=StringSource,
        is_required=False,
        description="Python module containing the target Dagster repository.",
    ),
}
LEGACY_LOCATION_CONFIG_SCHEMA = Shape(fields=LEGACY_CONFIG_SCHEMA_FIELDS)
LEGACY_NAMED_LOCATIONS_CONFIG_SCHEMA = Map(
    str, LEGACY_LOCATION_CONFIG_SCHEMA, key_label_name="location_name"
)
LEGACY_WORKSPACE_CONFIG_SCHEMA = Shape(
    fields={
        "locations": Field(
            LEGACY_NAMED_LOCATIONS_CONFIG_SCHEMA,
            description="Mapping from location names to location details.",
        )
    }
)

# The modern format is structured as
# locations:
#   - location_name: my_location_name
#     image: abc
#     code_source:
#       python_file: repo.py
LOCATION_CONFIG_SCHEMA = Shape(fields=CONFIG_SCHEMA_FIELDS)
WORKSPACE_CONFIG_SCHEMA = Shape(fields={"locations": [LOCATION_CONFIG_SCHEMA]})
