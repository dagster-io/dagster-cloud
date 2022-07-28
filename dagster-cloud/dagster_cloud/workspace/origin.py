from typing import Dict, NamedTuple

import dagster._check as check
from dagster._serdes import whitelist_for_serdes


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
