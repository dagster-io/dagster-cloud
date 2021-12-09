METADATA_ENTRY_FRAGMENT = """
fragment MetadataEntryFragment on EventMetadataEntry {
  __typename
  label
  description
  ... on EventPathMetadataEntry {
    path
  }
  ... on EventJsonMetadataEntry {
    jsonString
  }
  ... on EventUrlMetadataEntry {
    url
  }
  ... on EventTextMetadataEntry {
    text
  }
  ... on EventMarkdownMetadataEntry {
    mdStr
  }
  ... on EventPythonArtifactMetadataEntry {
    module
    name
  }
  ... on EventFloatMetadataEntry {
    floatValue
  }
  ... on EventIntMetadataEntry {
    intValue
  }
}
"""

PYTHON_ERROR_FRAGMENT = """
fragment PythonErrorFragment on PythonError {
  __typename
  message
  stack
  cause {
    message
    stack
  }
}
"""

SERIALIZABLE_ERROR_INFO_FRAGMENT = """

"""

EVENT_LOG_ENTRY_FRAGMENT = """
fragment EventLogEntryFragment on EventLogEntry {
    errorInfo
    message
    level
    userMessage
    runId
    timestamp
    stepKey
    pipelineName
    dagsterEvent
}
"""

EVENT_RECORD_FRAGMENT = (
    EVENT_LOG_ENTRY_FRAGMENT
    + """
    fragment EventLogRecordFragment on EventLogRecord {
        storageId
        eventLogEntry {
            ...EventLogEntryFragment
        }
    }
    """
)

GET_LOGS_FOR_RUN_QUERY = (
    EVENT_LOG_ENTRY_FRAGMENT
    + """
    query getLogsForRun($runId: String!, $cursor: Int, $ofType: String, $limit: Int) {
        eventLogs {
            getLogsForRun(runId: $runId, cursor: $cursor, ofType: $ofType, limit: $limit) {
                ...EventLogEntryFragment
            }
        }
    }
    """
)

GET_STATS_FOR_RUN_QUERY = """
    query getStatsForRun($runId: String!) {
        eventLogs {
            getStatsForRun(runId: $runId) {
                runId
                stepsSucceeded
                stepsFailed
                materializations
                expectations
                enqueuedTime
                launchTime
                startTime
                endTime
            }
        }
    }
    """

GET_STEP_STATS_FOR_RUN_QUERY = """
    query getStepStatsForRun($runId: String!, $stepKeys: [String!]) {
        eventLogs {
            getStepStatsForRun(runId: $runId, stepKeys: $stepKeys) {
                runId
                stepKey
                status
                startTime
                endTime
                materializations
                expectationResults
                attempts
            }
        }
    }
    """

IS_PERSISTENT_QUERY = """
    query isPersistent {
        eventLogs {
            isPersistent
        }
    }
    """

IS_ASSET_AWARE_QUERY = """
    query isAssetAware {
        eventLogs {
            isAssetAware
        }
    }
    """

GET_EVENT_RECORDS_QUERY = (
    EVENT_RECORD_FRAGMENT
    + """
    query getEventRecords($eventRecordsFilter: EventRecordsFilter, $limit: Int, $ascending: Boolean) {
        eventLogs {
            getEventRecords(
                eventRecordsFilter: $eventRecordsFilter,
                limit: $limit,
                ascending: $ascending,
            ) {
                ...EventLogRecordFragment
            }
        }
    }
    """
)
HAS_ASSET_KEY_QUERY = """
    query hasAssetKey($assetKey: String!) {
        eventLogs {
            hasAssetKey(assetKey: $assetKey)
        }
    }
    """

GET_ALL_ASSET_KEYS_QUERY = """
    query getAllAssetKeys {
        eventLogs {
            getAllAssetKeys
        }
    }
    """

GET_ALL_ASSET_TAGS_QUERY = """
    query getAllAssetTags() {
        eventLogs {
            getAllAssetTags
        }
    }
    """

GET_ASSET_RUN_IDS_QUERY = """
    query getAssetRunIds($assetKey: String!) {
        eventLogs {
            getAssetRunIds(assetKey: $assetKey)
        }
    }
    """

GET_ASSET_TAGS_QUERY = """
    query getAssetTags($assetKey: String!) {
        eventLogs {
            getAssetTags(assetKey: $assetKey)
        }
    }
"""

STORE_EVENT_MUTATION = """
    mutation StoreEvent($eventRecord: EventLogEntryInput!) {
        eventLogs {
            StoreEvent(eventRecord: $eventRecord) {
                ok
            }
        }
    }
    """

DELETE_EVENTS_MUTATION = """
    mutation DeleteEvents($runId: String!) {
        eventLogs {
            DeleteEvents(runId: $runId) {
                ok
            }
        }
    }
    """

UPGRADE_EVENT_LOG_STORAGE_MUTATION = """
    mutation UpgradeEventLogStorage {
        eventLogs {
            Upgrade {
                ok
            }
        }
    }
    """

REINDEX_MUTATION = """
    mutation Reindex {
        eventLogs {
            Reindex {
                ok
            }
        }
    }
"""

WIPE_EVENT_LOG_STORAGE_MUTATION = """
    mutation WipeEventLogStorage {
        eventLogs {
            Wipe {
                ok
            }
        }
    }
"""

WATCH_MUTATION = """
    mutation Watch($runId: String!, $startCursor: Int!, $callback: JSONString!) {
        eventLogs {
            Watch(runId: $runId, startCursor: $startCursor, callback: $callback) {
                ok
            }
        }
    }
"""

END_WATCH_MUTATION = """
    mutation EndWatch($runId: String!, $handler: JSONString!) {
        eventLogs {
            EndWatch(runId: $runId, handler: $handler) {
                ok
            }
        }
    }
"""

ENABLE_SECONDARY_INDEX_MUTATION = """
    mutation EnableSecondaryIndex($name: String!, $runId: String) {
        eventLogs {
            EnableSecondaryIndex(name: $name, runId: $runId) {
                ok
            }
        }
    }
"""

WIPE_ASSET_MUTATION = """
    mutation WipeAsset($assetKey: String!) {
        eventLogs {
            WipeAsset(assetKey: $assetKey) {
                ok
            }
        }
    }
"""
