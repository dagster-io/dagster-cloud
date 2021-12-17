from typing import Dict, List, NamedTuple, Optional

from dagster import check
from dagster.serdes import whitelist_for_serdes
from dagster.utils import merge_dicts


@whitelist_for_serdes
class CodeDeploymentMetadata(
    NamedTuple(
        "_CodeDeploymentMetadata",
        [
            ("image", Optional[str]),
            ("python_file", Optional[str]),
            ("package_name", Optional[str]),
            ("module_name", Optional[str]),
            ("working_directory", Optional[str]),
            ("executable_path", Optional[str]),
            ("attribute", Optional[str]),
        ],
    )
):
    def __new__(
        cls,
        image=None,
        python_file=None,
        package_name=None,
        module_name=None,
        working_directory=None,
        executable_path=None,
        attribute=None,
    ):
        check.invariant(
            len([val for val in [python_file, package_name, module_name] if val]) == 1,
            "Must supply exactly one of a file name, a package name, or a module name",
        )

        if working_directory:
            check.invariant(python_file, "Can only set a working directory if python_file is set")

        return super(CodeDeploymentMetadata, cls).__new__(
            cls,
            check.opt_str_param(image, "image"),
            check.opt_str_param(python_file, "python_file"),
            check.opt_str_param(package_name, "package_name"),
            check.opt_str_param(module_name, "module_name"),
            check.opt_str_param(working_directory, "working_directory"),
            check.opt_str_param(executable_path, "executable_path"),
            check.opt_str_param(attribute, "attribute"),
        )

    def get_grpc_server_command(self) -> List[str]:
        return ([self.executable_path, "-m"] if self.executable_path else []) + [
            "dagster",
            "api",
            "grpc",
        ]

    def get_grpc_server_env(self, port: int) -> Dict[str, str]:
        return merge_dicts(
            {
                "DAGSTER_CLI_API_GRPC_LAZY_LOAD_USER_CODE": "1",
                "DAGSTER_CLI_API_GRPC_HOST": "0.0.0.0",
                "DAGSTER_CLI_API_GRPC_PORT": str(port),
            },
            ({"DAGSTER_CURRENT_IMAGE": self.image} if self.image else {}),
            ({"DAGSTER_CLI_API_GRPC_PYTHON_FILE": self.python_file} if self.python_file else {}),
            ({"DAGSTER_CLI_API_GRPC_MODULE_NAME": self.module_name} if self.module_name else {}),
            ({"DAGSTER_CLI_API_GRPC_PACKAGE_NAME": self.package_name} if self.package_name else {}),
            (
                {"DAGSTER_CLI_API_GRPC_WORKING_DIRECTORY": self.working_directory}
                if self.working_directory
                else {"DAGSTER_CLI_API_GRPC_EMPTY_WORKING_DIRECTORY": "1"}
            ),
            ({"DAGSTER_CLI_API_GRPC_ATTRIBUTE": self.attribute} if self.attribute else {}),
        )
