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
        module_name = config.get("module_name")
        check.invariant(
            len([val for val in [python_file, package_name, module_name] if val]) == 1,
            "Must supply exactly one of a file name, a package name, or a module name",
        )

        if config.get("working_directory"):
            check.invariant(python_file, "Can only set a working directory if python_file is set")

    return True


LOCATION_CONFIG_SCHEMA = Shape(
    fields={
        "image": Field(config=StringSource, is_required=False),
        "python_file": Field(config=StringSource, is_required=False),
        "working_directory": Field(config=StringSource, is_required=False),
        "package_name": Field(config=StringSource, is_required=False),
        "module_name": Field(config=StringSource, is_required=False),
        "executable_path": Field(config=StringSource, is_required=False),
        "attribute": Field(config=StringSource, is_required=False),
    }
)
