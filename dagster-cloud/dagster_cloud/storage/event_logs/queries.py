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

ASSET_ENTRY_FRAGMENT = (
    EVENT_LOG_ENTRY_FRAGMENT
    + """
    fragment AssetEntryFragment on AssetEntry {
        assetKey {
            path
        }
        lastMaterialization {
            ...EventLogEntryFragment
        }
        lastRunId
        assetDetails {
            lastWipeTimestamp
        }
    }
    """
)

ASSET_RECORD_FRAGMENT = (
    ASSET_ENTRY_FRAGMENT
    + """
    fragment AssetRecordFragment on AssetRecord {
        storageId
        assetEntry {
            ...AssetEntryFragment
        }
    }
    """
)

GET_RECORDS_FOR_RUN_QUERY = (
    EVENT_RECORD_FRAGMENT
    + """
    query getRecordsForRun($runId: String!, $cursor: String, $ofType: String, $ofTypes: [String!], $limit: Int) {
        eventLogs {
            getRecordsForRun(runId: $runId, cursor: $cursor, ofType: $ofType, ofTypes: $ofTypes, limit: $limit) {
                records {
                    ...EventLogRecordFragment
                }
                cursor
                hasMore
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
                materializationEvents
                expectationResults
                attempts
                attemptsList
                markers
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

GET_LATEST_MATERIALIZATION_EVENTS_QUERY = (
    EVENT_LOG_ENTRY_FRAGMENT
    + """
    query getLatestMaterializationEvents($assetKeys: [String!]!) {
        eventLogs {
            getLatestMaterializationEvents(assetKeys: $assetKeys) {
                ...EventLogEntryFragment
            }
        }
    }
    """
)

GET_ASSET_RECORDS_QUERY = (
    ASSET_RECORD_FRAGMENT
    + """
    query getAssetRecords($assetKeys: [String!]) {
        eventLogs {
            getAssetRecords(assetKeys: $assetKeys) {
                ...AssetRecordFragment
            }
        }
    }
    """
)

GET_ALL_ASSET_KEYS_QUERY = """
    query getAllAssetKeys {
        eventLogs {
            getAllAssetKeys
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

UPDATE_ASSET_CACHED_STATUS_DATA_MUTATION = """
    mutation updateAssetCachedStatusData($cacheData: AssetStatusCacheValueInput!) {
        eventLogs {
            UpdateAssetCachedStatusData(cacheData: $cacheData) {
                ok
            }
        }
    }
"""

GET_MATERIALIZATION_COUNT_BY_PARTITION = """
    query getMaterializationCountByPartition($assetKeys: [String!]!) {
        eventLogs {
            getMaterializationCountByPartition(assetKeys: $assetKeys) {
                ... on AssetMaterializationCountByPartition {
                    assetKey
                    materializationCountByPartition {
                        partition
                        materializationCount
                    }
                }
            }
        }
    }
    """

GET_EVENT_TAGS_FOR_ASSET = """
query getAssetEventTags($assetKey: String!, $filterTags: [AssetFilterTagInput!], $filterEventId: Int) {
    eventLogs {
        getAssetEventTags(assetKey: $assetKey, filterTags: $filterTags, filterEventId: $filterEventId) {
            ... on AssetEventTags {
                tags {
                    key
                    value
                }
            }
        }
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
