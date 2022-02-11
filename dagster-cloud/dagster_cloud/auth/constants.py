from typing import Optional

API_TOKEN_HEADER = "Dagster-Cloud-Api-Token"

DEPLOYMENT_NAME_HEADER = "Dagster-Cloud-Deployment"


def get_hardcoded_test_agent_token(organization_name) -> str:
    return f"agent:{organization_name}:hardcoded"


def get_hardcoded_test_user_token(organization_name, user_name) -> str:
    return f"user:{organization_name}:{user_name}"


def get_organization_name_from_agent_token(agent_token: str) -> Optional[str]:
    split_token = agent_token.split(":")
    # Legacy agent token format - organization must be specified in dagster.yaml
    if len(split_token) == 1:
        return None

    _type, organization, _identifier = split_token

    return organization
