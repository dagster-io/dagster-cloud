from collections import namedtuple

from dagster import check
from dagster.serdes import whitelist_for_serdes


@whitelist_for_serdes
class CodeDeploymentMetadata(
    namedtuple("_CodeDeploymentMetadata", "image python_file package_name")
):
    def __new__(cls, image=None, python_file=None, package_name=None):
        check.invariant(
            (python_file or package_name) and not (python_file and package_name),
            "Must supply exactly one of a file name or a package name",
        )
        return super(CodeDeploymentMetadata, cls).__new__(
            cls,
            check.opt_str_param(image, "image"),
            check.opt_str_param(python_file, "python_file"),
            check.opt_str_param(package_name, "package_name"),
        )
