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

SEND_USER_CLOUD_RESPONSE_MUTATION = """
    mutation SendUserCloudResponse($requestId: String!, $requestApi: String!, $response: String!) {
        userCloudAgent {
            sendUserCloudAgentResponse(requestId: $requestId, requestApi: $requestApi, responseBody: $response) {
                requestId
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
            }
        }
    }
"""
