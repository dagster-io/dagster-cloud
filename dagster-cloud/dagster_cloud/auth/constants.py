from typing import Optional

from dagster._core.errors import DagsterInvariantViolationError


def get_hardcoded_test_agent_token(organization_name) -> str:
    return f"agent:{organization_name}:hardcoded"


def get_hardcoded_test_user_token(organization_name, user_name) -> str:
    return f"user:{organization_name}:{user_name}"


def get_organization_name_from_agent_token(agent_token: str) -> Optional[str]:
    split_token = agent_token.split(":")

    # Legacy agent token format - organization must be specified in dagster.yaml
    if len(split_token) == 1:
        return None

    token_type, *token = split_token

    if token_type == "user":
        raise DagsterInvariantViolationError(
            "Agent was configured with a user token, but agents can only authenticate with "
            "Dagster Cloud when configured with an agent token. "
            "Generate a new agent token in Dagster Cloud."
        )

    organization, _identifier = token

    return organization
