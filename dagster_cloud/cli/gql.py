from typing import Any, Dict, List, Optional

from dagster_cloud.api.client import GqlShimClient, create_cloud_dagit_client


def graphql_client_from_url(url: str, token: str) -> GqlShimClient:
    return create_cloud_dagit_client(url, token)


def url_from_config(organization: str, deployment: Optional[str] = None) -> str:
    """
    Gets the Cloud Dagit base url for a given organization and API token.
    Uses the default deployment if none is specified.
    """

    # Return the root URL / root GQL endpoint if no deployment is provided
    if not deployment:
        return f"https://{organization}.dagster.cloud"

    return f"https://{organization}.dagster.cloud/{deployment}"


DEPLOYMENTS_QUERY = """
{
    deployments {
        deploymentName
        deploymentId
    }
}
"""


def fetch_deployments(client: GqlShimClient) -> List[Any]:
    return client.execute(DEPLOYMENTS_QUERY)["data"]["deployments"]


class CliInputCodeLocation:
    def __init__(
        self,
        name: str,
        python_file: str = None,
        package_name: str = None,
        image: str = None,
        module_name: str = None,
        working_directory: str = None,
        executable_path: str = None,
        attribute: str = None,
        commit_hash: str = None,
        url: str = None,
    ):
        self.name = name

        if len([val for val in [python_file, package_name, module_name] if val]) != 1:
            raise Exception(
                "Must specify exactly one of --python-file or --package-name or --module-name."
            )

        self.python_file = python_file
        self.package_name = package_name
        self.image = image
        self.module_name = module_name
        self.working_directory = working_directory
        self.executable_path = executable_path
        self.attribute = attribute
        self.commit_hash = commit_hash
        self.url = url

    def get_location_input(self):
        location_input = {"name": self.name}

        if self.python_file:
            location_input["pythonFile"] = self.python_file
        if self.package_name:
            location_input["packageName"] = self.package_name
        if self.image:
            location_input["image"] = self.image
        if self.module_name:
            location_input["moduleName"] = self.module_name
        if self.working_directory:
            location_input["workingDirectory"] = self.working_directory
        if self.executable_path:
            location_input["executablePath"] = self.executable_path
        if self.attribute:
            location_input["attribute"] = self.attribute
        if self.commit_hash:
            location_input["commitHash"] = self.commit_hash
        if self.url:
            location_input["url"] = self.url

        return location_input


AGENT_STATUS_QUERY = """
query AgentStatus {
    agents {
        status
        errors {
            error {
                message
            }
        }
    }
}
"""


def fetch_agent_status(client: GqlShimClient) -> List[Any]:
    return client.execute(AGENT_STATUS_QUERY)["data"]["agents"]


WORKSPACE_ENTRIES_QUERY = """
query WorkspaceEntries {
    workspace {
        workspaceEntries {
            locationName
            serializedDeploymentMetadata
        }
    }
}
"""


def fetch_workspace_entries(client: GqlShimClient) -> List[Any]:
    return client.execute(WORKSPACE_ENTRIES_QUERY)["data"]["workspace"]["workspaceEntries"]


REPOSITORY_LOCATIONS_QUERY = """
{
  workspaceOrError {
    __typename
    ... on Workspace {
        locationEntries {
            __typename
            name
            loadStatus
            locationOrLoadError {
                __typename
                ... on RepositoryLocation {
                    name
                }
                ... on PythonError {
                    message
                    stack
                }
            }
        }
    }
    ... on PythonError {
        message
        stack
    }
  }
}
"""


def fetch_code_locations(client: GqlShimClient) -> List[Any]:
    result = client.execute(REPOSITORY_LOCATIONS_QUERY)["data"]["workspaceOrError"]
    if result["__typename"] != "Workspace":
        raise Exception("Unable to query repository locations: ", result["message"])
    return result["locationEntries"]


UPDATE_LOCATION_MUTATION = """
mutation ($location: LocationSelector!) {
    updateLocation(location: $location) {
        __typename
            ... on WorkspaceEntry {
            locationName
        }
        ... on PythonError {
            message
            stack
        }
    }
}
"""


def update_code_location(client: GqlShimClient, location_details: CliInputCodeLocation) -> None:
    result = client.execute(
        UPDATE_LOCATION_MUTATION,
        variable_values={"location": location_details.get_location_input()},
    )["data"]["updateLocation"]
    if result["__typename"] != "WorkspaceEntry":
        raise Exception("Unable to update code location: ", result["message"])


ADD_LOCATION_MUTATION = """
mutation ($location: LocationSelector!) {
    addLocation(location: $location) {
        __typename
            ... on WorkspaceEntry {
            locationName
        }
        ... on PythonError {
            message
            stack
        }
    }
}
"""


def add_code_location(client: GqlShimClient, location_details: CliInputCodeLocation) -> None:
    result = client.execute(
        ADD_LOCATION_MUTATION, variable_values={"location": location_details.get_location_input()}
    )["data"]["addLocation"]
    if result["__typename"] != "WorkspaceEntry":
        raise Exception("Unable to add code location: ", result["message"])


ADD_OR_UPDATE_LOCATION_MUTATION = """
mutation ($location: LocationSelector!) {
    addOrUpdateLocation(location: $location) {
        __typename
        ... on WorkspaceEntry {
            locationName
        }
        ... on PythonError {
            message
            stack
        }
    }
}
"""


def add_or_update_code_location(
    client: GqlShimClient, location_details: CliInputCodeLocation
) -> None:
    result = client.execute(
        ADD_OR_UPDATE_LOCATION_MUTATION,
        variable_values={"location": location_details.get_location_input()},
    )["data"]["addOrUpdateLocation"]
    if result["__typename"] != "WorkspaceEntry":
        raise Exception("Unable to add/update code location: ", result["message"])


DELETE_LOCATION_MUTATION = """
mutation ($locationName: String!) {
    deleteLocation(locationName: $locationName) {
        __typename
        ... on DeleteLocationSuccess {
            locationName
        }
        ... on PythonError {
            message
            stack
        }
    }
}
"""


def delete_code_location(client: GqlShimClient, location_name: str) -> None:
    result = client.execute(
        DELETE_LOCATION_MUTATION, variable_values={"locationName": location_name}
    )

    if result["data"]["deleteLocation"]["__typename"] != "DeleteLocationSuccess":
        raise Exception(f"Unable to delete location: {str(result['data']['deleteLocation'])}")


RECONCILE_LOCATIONS_MUTATION = """
mutation ($locations: [LocationSelector]!) {
    reconcileLocations(locations: $locations) {
        __typename
        ... on ReconcileLocationsSuccess {
            locations {
                locationName
            }
        }
        ... on PythonError {
            message
            stack
        }
    }
}
"""


def reconcile_code_locations(
    client: GqlShimClient, locations: List[CliInputCodeLocation]
) -> List[str]:
    result = client.execute(
        RECONCILE_LOCATIONS_MUTATION,
        variable_values={"locations": [location.get_location_input() for location in locations]},
    )

    if result["data"]["reconcileLocations"]["__typename"] == "ReconcileLocationsSuccess":
        return sorted(
            [
                location["locationName"]
                for location in result["data"]["reconcileLocations"]["locations"]
            ]
        )
    else:
        raise Exception(f"Unable to sync locations: {str(result)}")


SET_DEPLOYMENT_SETTINGS_MUTATION = """
    mutation SetDeploymentSettings($deploymentSettings: DeploymentSettingsInput!) {
        setDeploymentSettings(deploymentSettings: $deploymentSettings) {
            __typename
            ... on DeploymentSettings {
                settings
            }
            ...on UnauthorizedError {
                message
            }
            ... on PythonError {
                message
                stack
            }
        }
    }
"""


def set_deployment_settings(client: GqlShimClient, deployment_settings: Dict[str, Any]) -> None:
    result = client.execute(
        SET_DEPLOYMENT_SETTINGS_MUTATION,
        variable_values={"deploymentSettings": deployment_settings},
    )

    if result["data"]["setDeploymentSettings"]["__typename"] != "DeploymentSettings":
        raise Exception(f"Unable to set deployment settings: {str(result)}")


DEPLOYMENT_SETTINGS_QUERY = """
    query DeploymentSettings {
        deploymentSettings {
            settings
        }
    }
"""


def get_deployment_settings(client: GqlShimClient) -> Dict[str, Any]:
    result = client.execute(DEPLOYMENT_SETTINGS_QUERY)

    if result.get("data", {}).get("deploymentSettings", {}).get("settings") == None:
        raise Exception(f"Unable to get deployment settings: {str(result)}")

    return result["data"]["deploymentSettings"]["settings"]
