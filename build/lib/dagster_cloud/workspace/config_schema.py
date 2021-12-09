from dagster import check
from dagster.config import Field, Shape
from dagster.config.source import StringSource
from dagster.config.validate import process_config


def process_workspace_config(workspace_config):
    check.dict_param(workspace_config, "workspace_config")
    check.is_dict(workspace_config.get("locations"))

    for _, config in workspace_config["locations"].items():
        validation = process_config(LOCATION_CONFIG_SCHEMA, config)
        check.invariant(
            validation.success,
            ", ".join([error.message for error in validation.errors]),
        )

        python_file = config.get("python_file")
        package_name = config.get("package_name")
        check.invariant(
            (python_file or package_name) and not (python_file and package_name),
            "Must supply exactly one of a file name or a package name",
        )

    return True


LOCATION_CONFIG_SCHEMA = Shape(
    fields={
        "image": Field(config=StringSource, is_required=False),
        "python_file": Field(config=StringSource, is_required=False),
        "package_name": Field(config=StringSource, is_required=False),
    }
)
