GET_USER_CLOUD_REQUESTS_QUERY = """
    mutation GetUserCloudRequests {
        userCloudAgent {
            popUserCloudAgentRequests(limit:10) {
                requestId
                requestApi
                requestBody
            }
        }
    }
"""

WORKSPACE_ENTRIES_QUERY = """
    query WorkspaceEntries {
        workspace {
            workspaceEntries {
                locationName
                serializedDeploymentMetadata
                hasOutdatedData
                metadataTimestamp
                sandboxSavedTimestamp
                sandboxProxyInfo {
                    hostname
                    port
                    authToken
                    minPort
                    maxPort
                }
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
