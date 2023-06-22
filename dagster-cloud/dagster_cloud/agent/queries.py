GET_USER_CLOUD_REQUESTS_QUERY = """
    mutation GetUserCloudRequests($forBranchDeployments: Boolean $forFullDeployments: [String!], $limit: Int) {
        userCloudAgent {
            popUserCloudAgentRequests(limit: $limit, forBranchDeployments: $forBranchDeployments, forFullDeployments: $forFullDeployments) {
                requestId
                requestApi
                requestBody
                deploymentName
                isBranchDeployment
            }
        }
    }

"""

DEPLOYMENTS_QUERY = """
    query Deployments($deploymentNames: [String!]!) {
        deployments(deploymentNames: $deploymentNames) {
            deploymentName
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

GET_AGENTS_QUERY = """
    query Agents($heartbeatedSince: Float) {
        agents(heartbeatedSince: $heartbeatedSince) {
            id
            status
        }
    }
"""
