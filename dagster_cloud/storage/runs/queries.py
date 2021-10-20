ERROR_FRAGMENT = """
fragment errorFragment on PythonError {
  message
  className
  stack
  cause {
    message
    className
    stack
    cause {
      message
      className
      stack
    }
  }
}
"""

RUN_ROW_FRAGMENT = """
fragment runRecordFragment on RunRecord {
    storageId
    serializedPipelineRun
    createTimestamp
    updateTimestamp
}
"""

ADD_RUN_MUTATION = (
    ERROR_FRAGMENT
    + """
    mutation addRunMutation($serializedPipelineRun: String!) {
        runs {
            addRun(serializedPipelineRun: $serializedPipelineRun) {
                ok
                error {
                    ...errorFragment
                }
            }
        }
    }
"""
)

HANDLE_RUN_EVENT_MUTATION = """
    mutation handleRunEventMutation($runId: String!, $serializedEvent: String!) {
        runs {
            handleRunEvent(runId: $runId, serializedEvent: $serializedEvent) {
                ok
            }
        }
    }
"""

GET_RUNS_QUERY = """
    query getRunsQuery($filters: PipelineRunsFilter, $cursor: String, $limit: Int) {
        runs {
            getRuns(filters: $filters, cursor: $cursor, limit: $limit)
        }
    }
"""

GET_RUNS_COUNT_QUERY = """
    query getRunsCountQuery($filters: PipelineRunsFilter) {
        runs {
            getRunsCount(filters: $filters)
        }
    }
"""

GET_RUN_BY_ID_QUERY = """
    query getRunByIdQuery($runId: String!) {
        runs {
            getRunById(runId: $runId)
        }
    }
"""

HAS_RUN_QUERY = """
    query hasRunQuery($runId: String!) {
        runs {
            hasRun(runId: $runId)
        }
    }
"""

HAS_PIPELINE_SNAPSHOT_QUERY = """
    query hasPipelineSnapshotQuery($pipelineSnapshotId: String!) {
        runs {
            hasPipelineSnapshot(pipelineSnapshotId: $pipelineSnapshotId)
        }
    }
"""

GET_PIPELINE_SNAPSHOT_QUERY = """
    query getPipelineSnapshotQuery($pipelineSnapshotId: String!) {
        runs {
            getPipelineSnapshot(pipelineSnapshotId: $pipelineSnapshotId)
        }
    }
"""

GET_RUN_GROUP_QUERY = """
    query getRunGroupQuery($runId: String!) {
        runs {
            getRunGroupOrError(runId: $runId) {
                __typename
                ... on SerializedRunGroup {
                  rootRunId
                  serializedRuns
                }
                ... on PipelineRunNotFoundError {
                    runId
                }
            }
        }
    }
"""


GET_RUN_GROUPS_QUERY = """
    query getRunGroupsQuery($filters: PipelineRunsFilter, $cursor: String, $limit: Int) {
        runs {
            getRunGroups(filters: $filters, cursor: $cursor, limit: $limit) {
                rootRunId
                serializedRuns
                count
            }
        }
    }
"""

GET_RUN_RECORDS_QUERY = (
    RUN_ROW_FRAGMENT
    + """
    query getRunRecordsQuery($filters: PipelineRunsFilter, $limit: Int, $orderBy: String, $ascending: Boolean) {
        runs {
            getRunRecords(filters: $filters, limit: $limit, orderBy: $orderBy, ascending: $ascending) {
                ...runRecordFragment
            }
        }
    }
"""
)

GET_RUN_TAGS_QUERY = """
    query getRunTagsQuery {
        runs {
            getRunTags {
                key
                values
            }
        }
    }
"""

ADD_RUN_TAGS_MUTATION = """
    mutation addRunTagsMutation($runId: String!, $jsonNewTags: JSONString!) {
        runs {
            addRunTags(runId: $runId, jsonNewTags: $jsonNewTags) {
                ok
            }
        }
    }
"""

ADD_PIPELINE_SNAPSHOT_MUTATION = """
    mutation addPipelineSnapshotMutation($serializedPipelineSnapshot: String!) {
        runs {
            addPipelineSnapshot(serializedPipelineSnapshot: $serializedPipelineSnapshot) {
                ok
            }
        }
    }
"""

HAS_EXECUTION_PLAN_SNAPSHOT_QUERY = """
    query hasExecutionPlanSnapshotQuery($executionPlanSnapshotId: String!) {
        runs {
            hasExecutionPlanSnapshot(executionPlanSnapshotId: $executionPlanSnapshotId)
        }
    }
"""

ADD_EXECUTION_PLAN_SNAPSHOT_MUTATION = """
    mutation addExecutionPlanSnapshotMutation($serializedExecutionPlanSnapshot: String!) {
        runs {
            addExecutionPlanSnapshot(serializedExecutionPlanSnapshot: $serializedExecutionPlanSnapshot) {
                ok
            }
        }
    }
"""

GET_EXECUTION_PLAN_SNAPSHOT_QUERY = """
    query getExecutionPlanSnapshotQuery($executionPlanSnapshotId: String!) {
        runs {
            getExecutionPlanSnapshot(executionPlanSnapshotId: $executionPlanSnapshotId)
        }
    }
"""

ADD_DAEMON_HEARTBEAT_MUTATION = """
    mutation addDaemonHeartbeat($serializedDaemonHeartbeat: String!) {
        runs {
            addDaemonHeartbeat(serializedDaemonHeartbeat: $serializedDaemonHeartbeat) {
                ok
            }
        }
    }
"""

GET_DAEMON_HEARTBEATS_QUERY = """
    query getDaemonHeartbeatsQuery {
        runs {
            getDaemonHeartbeats
        }
    }
"""

GET_BACKFILLS_QUERY = """
    query getBackfillsQuery($status: String, $cursor: String, $limit: Int) {
        runs {
            getBackfills(status: $status, cursor: $cursor, limit: $limit)
        }
    }
"""

GET_BACKFILL_QUERY = """
    query getBackfillQuery($backfillId: String!) {
        runs {
            getBackfill(backfillId: $backfillId)
        }
    }
"""

ADD_BACKFILL_MUTATION = """
    mutation addBackfill($serializedPartitionBackfill: String!) {
        runs {
            addBackfill(serializedPartitionBackfill: $serializedPartitionBackfill) {
                ok
            }
        }
    }
"""

UPDATE_BACKFILL_MUTATION = """
    mutation updateBackfill($serializedPartitionBackfill: String!) {
        runs {
            updateBackfill(serializedPartitionBackfill: $serializedPartitionBackfill) {
                ok
            }
        }
    }
"""
