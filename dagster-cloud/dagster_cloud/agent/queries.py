GET_USER_CLOUD_REQUESTS_QUERY = """
    mutation GetUserCloudRequests($forBranchDeployments: Boolean) {
        userCloudAgent {
            popUserCloudAgentRequests(limit:10, forBranchDeployments: $forBranchDeployments) {
                requestId
                requestApi
                requestBody
                deploymentName
            }
        }
    }
"""

WORKSPACE_ENTRIES_QUERY = """
    query WorkspaceEntries($deploymentNames: [String!]!) {
        deployments(deploymentNames: $deploymentNames) {
            deploymentName
            workspaceEntries {
                locationName
                serializedDeploymentMetadata
                hasOutdatedData
                metadataTimestamp
                sandboxSavedTimestamp
            }
        }
    }
"""

ADD_AGENT_HEARTBEAT_MUTATION = """
    mutation AddAgentHeartbeat($serializedAgentHeartbeat: String!) {
        userCloudAgent {
            addAgentHeartbeat (serializedAgentHeartbeat: $serializedAgentHeartbeat) {
                ok
            }
        }
    }
"""

ADD_AGENT_HEARTBEATS_MUTATION = """
    mutation AddAgentHeartbeats($serializedAgentHeartbeat: String!, $deploymentNames: [String]) {
        userCloudAgent {
            addAgentHeartbeats (serializedAgentHeartbeat: $serializedAgentHeartbeat, deploymentNames: $deploymentNames) {
                ok
            }
        }
    }
"""
