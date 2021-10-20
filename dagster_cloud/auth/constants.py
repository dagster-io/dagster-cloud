API_TOKEN_HEADER = "Dagster-Cloud-Api-Token"

DEPLOYMENT_NAME_HEADER = "Dagster-Cloud-Deployment"


def get_hardcoded_test_agent_token(organization_name):
    return f"agent-test_agent_token_{organization_name}"


def get_hardcoded_test_user_token(organization_name, user_name):
    return f"user-test_agent_token_{organization_name}_{user_name}"
