GET_USER_CLOUD_REQUESTS_QUERY = """
    mutation GetUserCloudRequests($forBranchDeployments: Boolean) {
        userCloudAgent {
            popUserCloudAgentRequests(limit:10, forBranchDeployments: $forBranchDeployments) {
                requestId
                requestApi
                requestBody
                deploymentName
                isBranchDeployment
            }
        }
    }
"""

WORKSPACE_ENTRIES_QUERY = """
    query WorkspaceEntries($deploymentNames: [String!]!, $includeAllServerlessDeployments: Boolean!) {
        deployments(deploymentNames: $deploymentNames, includeAllServerlessDeployments: $includeAllServerlessDeployments) {
            deploymentName
            isBranchDeployment
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


ADD_AGENT_HEARTBEATS_MUTATION = """
    mutation AddAgentHeartbeats($serializedAgentHeartbeats: [AgentHeartbeatInput!]) {
        userCloudAgent {
            addAgentHeartbeats (serializedAgentHeartbeats: $serializedAgentHeartbeats) {
                ok
            }
        }
    }
"""
