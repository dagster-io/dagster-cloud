import logging
from typing import Optional

from dagster_cloud_cli import gql

from . import github_context


def create_or_update_branch_deployment_from_github_context(
    dagster_cloud_org_url: str,
    dagster_cloud_api_token: str,
    github_event: github_context.GithubEvent,
    mark_closed: bool,
) -> Optional[str]:
    """Return the branch deployment associated with the github PR."""
    event = github_event
    logging.debug("Read github event GithubEvent(%r)", event.__dict__)
    if not event.branch_name:
        logging.info("Not in a branch, not creating branch deployment")
        return None
    else:
        url = f"{dagster_cloud_org_url}"
        with gql.graphql_client_from_url(url, dagster_cloud_api_token) as client:
            deployment_name = gql.create_or_update_branch_deployment(
                client,
                repo_name=event.repo_name,
                branch_name=event.branch_name,
                commit_hash=event.github_sha,
                timestamp=event.timestamp,
                branch_url=event.branch_url,
                pull_request_url=event.pull_request_url,
                pull_request_status="CLOSED" if mark_closed else event.pull_request_status,
                pull_request_number=event.pull_request_id,
                author_name=event.author_name,
                author_email=event.author_email,
                commit_message=event.commit_msg,
                author_avatar_url=github_event.get_github_avatar_url(),
            )
        logging.info(
            "Got branch deployment %r for branch %r",
            deployment_name,
            event.branch_name,
        )
        return deployment_name
