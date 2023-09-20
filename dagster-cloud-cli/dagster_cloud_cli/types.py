from contextlib import suppress
from enum import Enum, EnumMeta
from typing import List, Tuple


class CliEventType(Enum):
    DEPLOY = "deploy"
    UPLOAD = "upload"
    BUILD = "build"


def tags(key: str, values: List[str]):
    return Enum(key, {value.replace("-", "_"): f"{key}:{value}" for value in values})


class CliEventTags:
    @classmethod
    def enums(cls):
        attrs = []
        for attr in cls.__dict__.values():
            if isinstance(attr, EnumMeta):
                attrs.append(attr)
        return attrs

    @classmethod
    def contains(cls, value) -> bool:
        for enum in cls.enums():
            with suppress(ValueError):
                if enum(value):
                    return True
        return False

    @classmethod
    def split(cls, value: str) -> Tuple[str, str]:
        if cls.contains(value):
            parts = value.split(":", 1)
            return (parts[0], parts[1])
        raise ValueError("Invalid tag value", value)

    agent_strategy = tags("agent-strategy", ["hybrid", "serverless"])
    server_strategy = tags("server-strategy", ["docker", "pex"])
    source = tags(
        "source",
        [
            "bitbucket",
            "buildkite",
            "circle-ci",
            "cli",
            "codebuild",
            "github",
            "gitlab",
            "jenkins",
            "travis",
            "unknown",
        ],
    )
    subcommand = tags(
        "subcommand",
        ["dagster-cloud-ci", "dagster-cloud-serverless"],
    )
