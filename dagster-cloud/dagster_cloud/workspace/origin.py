from typing import Any, Dict, List, NamedTuple, Optional

import dagster._check as check
from dagster.serdes import whitelist_for_serdes
from dagster.utils import merge_dicts


@whitelist_for_serdes
class GitMetadata(
    NamedTuple(
        "_GitMetadata",
        [("commit_hash", Optional[str]), ("url", Optional[str])],
    )
):
    def __new__(cls, commit_hash=None, url=None):
        return super(GitMetadata, cls).__new__(
            cls,
            check.opt_str_param(commit_hash, "commit_hash"),
            check.opt_str_param(url, "url"),
        )


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
            ("git_metadata", Optional[GitMetadata]),
            ("container_context", Optional[Dict[str, Any]]),
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
        git_metadata=None,
        container_context=None,
    ):
        check.invariant(
            len([val for val in [python_file, package_name, module_name] if val]) == 1,
            "Must supply exactly one of a file name, a package name, or a module name",
        )

        return super(CodeDeploymentMetadata, cls).__new__(
            cls,
            check.opt_str_param(image, "image"),
            check.opt_str_param(python_file, "python_file"),
            check.opt_str_param(package_name, "package_name"),
            check.opt_str_param(module_name, "module_name"),
            check.opt_str_param(working_directory, "working_directory"),
            check.opt_str_param(executable_path, "executable_path"),
            check.opt_str_param(attribute, "attribute"),
            check.opt_inst_param(git_metadata, "git_metadata", GitMetadata),
            check.opt_dict_param(container_context, "container_context", key_type=str),
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
                else {}
            ),
            ({"DAGSTER_CLI_API_GRPC_ATTRIBUTE": self.attribute} if self.attribute else {}),
            (
                {"DAGSTER_CLI_API_GRPC_USE_PYTHON_ENVIRONMENT_ENTRY_POINT": "1"}
                if self.executable_path
                else {}
            ),
        )


@whitelist_for_serdes
class CodePreviewMetadata(
    NamedTuple(
        "_CodePreviewMetadata",
        [
            ("commit_message", str),
            ("branch_name", str),
            ("branch_url", str),
            ("commit_sha", str),
            ("commit_url", str),
        ],
    )
):
    def __new__(
        cls,
        commit_message: str,
        branch_name: str,
        branch_url: str,
        commit_sha: str,
        commit_url: str,
    ):
        return super(CodePreviewMetadata, cls).__new__(
            cls,
            check.str_param(commit_message, "commit_message"),
            check.str_param(branch_name, "branch_name"),
            check.str_param(branch_url, "branch_url"),
            check.str_param(commit_sha, "commit_sha"),
            check.str_param(commit_url, "commit_url"),
        )

    @property
    def display_metadata(self) -> Dict[str, str]:
        return {
            "commit_message": self.commit_message,
            "branch_name": self.branch_name,
            "branch_url": self.branch_url,
            "commit_sha": self.commit_sha,
            "commit_url": self.commit_url,
        }
