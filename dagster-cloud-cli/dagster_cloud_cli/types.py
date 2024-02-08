from contextlib import suppress
from enum import Enum, EnumMeta
from typing import Tuple


class CliEventType(Enum):
    DEPLOY = "deploy"
    UPLOAD = "upload"
    BUILD = "build"


AgentStrategyTags = Enum(
    "agent-strategy", {"hybrid": "agent-strategy:hybrid", "serverless": "agent-strategy:serverless"}
)
ServerStrategyTags = Enum(
    "server-strategy", {"docker": "server-strategy:docker", "pex": "server-strategy:pex"}
)
SourceTags = Enum(
    "source",
    {
        "bitbucket": "source:bitbucket",
        "buildkite": "source:buildkite",
        "circle_ci": "source:circle-ci",
        "cli": "source:cli",
        "codebuild": "source:codebuild",
        "github": "source:github",
        "gitlab": "source:gitlab",
        "jenkins": "source:jenkins",
        "travis": "source:travis",
        "unknown": "source:unknown",
    },
)
SubcommandTags = Enum(
    "subcommand",
    {
        "dagster_cloud_ci": "subcommand:dagster-cloud-ci",
        "dagster_cloud_serverless": "subcommand:dagster-cloud-serverless",
    },
)


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

    agent_strategy = AgentStrategyTags
    server_strategy = ServerStrategyTags
    source = SourceTags
    subcommand = SubcommandTags
