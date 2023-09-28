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
  className
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

EVENT_RECORD_FRAGMENT = EVENT_LOG_ENTRY_FRAGMENT + """
    fragment EventLogRecordFragment on EventLogRecord {
        storageId
        eventLogEntry {
            ...EventLogEntryFragment
        }
    }
    """

CACHED_STATUS_DATA_FRAGMENT = """
fragment CachedStatusDataFragment on AssetStatusCacheValue {
    latestStorageId
    partitionsDefId
    serializedMaterializedPartitionSubset
    serializedFailedPartitionSubset
    serializedInProgressPartitionSubset
    earliestInProgressMaterializationEventId
}
"""

ASSET_ENTRY_FRAGMENT = EVENT_RECORD_FRAGMENT + CACHED_STATUS_DATA_FRAGMENT + """
    fragment AssetEntryFragment on AssetEntry {
        assetKey {
            path
        }
        lastMaterializationRecord {
            ...EventLogRecordFragment
        }
        lastRunId
        assetDetails {
            lastWipeTimestamp
        }
        cachedStatus {
            ...CachedStatusDataFragment
        }
    }
    """

ASSET_RECORD_FRAGMENT = ASSET_ENTRY_FRAGMENT + """
    fragment AssetRecordFragment on AssetRecord {
        storageId
        assetEntry {
            ...AssetEntryFragment
        }
    }
    """

GET_RECORDS_FOR_RUN_QUERY = EVENT_RECORD_FRAGMENT + """
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

GET_EVENT_RECORDS_QUERY = EVENT_RECORD_FRAGMENT + """
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
HAS_ASSET_KEY_QUERY = """
    query hasAssetKey($assetKey: String!) {
        eventLogs {
            hasAssetKey(assetKey: $assetKey)
        }
    }
    """

GET_LATEST_MATERIALIZATION_EVENTS_QUERY = EVENT_LOG_ENTRY_FRAGMENT + """
    query getLatestMaterializationEvents($assetKeys: [String!]!) {
        eventLogs {
            getLatestMaterializationEvents(assetKeys: $assetKeys) {
                ...EventLogEntryFragment
            }
        }
    }
    """

GET_ASSET_RECORDS_QUERY = ASSET_RECORD_FRAGMENT + """
    query getAssetRecords($assetKeys: [String!]) {
        eventLogs {
            getAssetRecords(assetKeys: $assetKeys) {
                ...AssetRecordFragment
            }
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

UPDATE_ASSET_CACHED_STATUS_DATA_MUTATION = """
    mutation updateAssetCachedStatusData($assetKey: String!, $cacheValues: AssetStatusCacheValueInput!) {
        eventLogs {
            UpdateAssetCachedStatusData(assetKey: $assetKey, cacheValues: $cacheValues) {
                ok
            }
        }
    }
"""

GET_MATERIALIZED_PARTITIONS = """
    query getMaterializedPartitions($assetKey: String!, $beforeCursor: BigInt, $afterCursor: BigInt) {
        eventLogs {
            getMaterializedPartitions(assetKey: $assetKey, beforeCursor: $beforeCursor, afterCursor: $afterCursor)
        }
    }
"""

GET_MATERIALIZATION_COUNT_BY_PARTITION = """
    query getMaterializationCountByPartition($assetKeys: [String!]!, $afterCursor: BigInt) {
        eventLogs {
            getMaterializationCountByPartition(assetKeys: $assetKeys, afterCursor: $afterCursor) {
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

GET_LATEST_STORAGE_ID_BY_PARTITION = """
    query getLatestStorageIdByPartition($assetKey: String!, $eventType: String!) {
        eventLogs {
            getLatestStorageIdByPartition(assetKey: $assetKey, eventType: $eventType) {
                ... on StorageIdByPartition {
                    partition
                    storageId
                }
            }
        }
    }
"""

GET_LATEST_TAGS_BY_PARTITION = """
    query getLatestTagsByPartition(
        $assetKey: String!,
        $eventType: String!,
        $tagKeys: [String!],
        $assetPartitions: [String!],
        $beforeCursor: BigInt,
        $afterCursor: BigInt,
    ) {
        eventLogs {
            getLatestTagsByPartition(
                assetKey: $assetKey,
                eventType: $eventType,
                tagKeys: $tagKeys,
                assetPartitions: $assetPartitions,
                beforeCursor: $beforeCursor,
                afterCursor: $afterCursor,
            ) {
                ... on TagByPartition {
                    partition
                    key
                    value
                }
            }
        }
    }
"""

GET_LATEST_ASSET_PARTITION_MATERIALIZATION_ATTEMPTS_WITHOUT_MATERIALIZATIONS = """
    query getLatestAssetPartitionMaterializationAttemptsWithoutMaterializations($assetKey: String!) {
        eventLogs {
            getLatestAssetPartitionMaterializationAttemptsWithoutMaterializations(assetKey: $assetKey)
        }
    }
    """

GET_EVENT_TAGS_FOR_ASSET = """
query getAssetEventTags($assetKey: String!, $filterTags: [AssetFilterTagInput!], $filterEventId: BigInt) {
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

WIPE_ASSET_CACHED_STATUS_DATA_MUTATION = """
    mutation WipeAssetCachedStatusData($assetKey: String!) {
        eventLogs {
            WipeAssetCachedStatusData(assetKey: $assetKey) {
                ok
            }
        }
    }
"""

GET_DYNAMIC_PARTITIONS_QUERY = """
query getDynamicPartitions($partitionsDefName: String!) {
    eventLogs {
        getDynamicPartitions(partitionsDefName: $partitionsDefName)
    }
}
"""

HAS_DYNAMIC_PARTITION_QUERY = """
query hasDynamicPartition($partitionsDefName: String!, $partitionKey: String!) {
    eventLogs {
        hasDynamicPartition(partitionsDefName: $partitionsDefName, partitionKey: $partitionKey)
    }
}
"""

ADD_DYNAMIC_PARTITIONS_MUTATION = """
    mutation AddDynamicPartitions($partitionsDefName: String!, $partitionKeys: [String!]!) {
        eventLogs {
            AddDynamicPartitions(partitionsDefName: $partitionsDefName, partitionKeys: $partitionKeys) {
                ok
            }
        }
    }
"""

DELETE_DYNAMIC_PARTITION_MUTATION = """
    mutation DeleteDynamicPartition($partitionsDefName: String!, $partitionKey: String!) {
        eventLogs {
            DeleteDynamicPartition(partitionsDefName: $partitionsDefName, partitionKey: $partitionKey) {
                ok
            }
        }
    }
"""

SET_CONCURRENCY_SLOTS_MUTATION = PYTHON_ERROR_FRAGMENT + """
mutation SetConcurrencySlots($concurrencyKey: String!, $num: Int!) {
    eventLogs {
        SetConcurrencySlots(concurrencyKey: $concurrencyKey, num: $num) {
            success
            error {
                ...PythonErrorFragment
            }
        }
    }
}
"""

GET_CONCURRENCY_KEYS_QUERY = """
query getConcurrencyKeys {
    eventLogs {
        getConcurrencyKeys
    }
}
"""

GET_CONCURRENCY_INFO_QUERY = """
query getConcurrencyInfo($concurrencyKey: String!) {
    eventLogs {
        getConcurrencyInfo(concurrencyKey: $concurrencyKey) {
            concurrencyKey
            slotCount
            activeSlotCount
            activeRunIds
            pendingStepCount
            pendingStepRunIds
            assignedStepCount
            assignedStepRunIds
        }
    }
}
"""

CLAIM_CONCURRENCY_SLOT_MUTATION = """
mutation ClaimConcurrencySlot($concurrencyKey: String!, $runId: String!, $stepKey: String!, $priority: Int!) {
    eventLogs {
        ClaimConcurrencySlot(concurrencyKey: $concurrencyKey, runId: $runId, stepKey: $stepKey, priority: $priority) {
            status {
                slotStatus
                priority
                assignedTimestamp
                enqueuedTimestamp
            }
        }
    }
}
"""

CHECK_CONCURRENCY_CLAIM_QUERY = """
query getCheckConcurrencyClaim($concurrencyKey: String!, $runId: String!, $stepKey: String!) {
    eventLogs {
        getCheckConcurrencyClaim(concurrencyKey: $concurrencyKey, runId: $runId, stepKey: $stepKey) {
            slotStatus
            priority
            assignedTimestamp
            enqueuedTimestamp
        }
    }
}
"""

FREE_CONCURRENCY_SLOTS_FOR_RUN_MUTATION = """
mutation FreeConcurrencySlotsForRun($runId: String!) {
    eventLogs {
        FreeConcurrencySlotsForRun(runId: $runId) {
            success
        }
    }
}
"""

FREE_CONCURRENCY_SLOT_FOR_STEP_MUTATION = """
mutation FreeConcurrencySlotForStep($runId: String!, $stepKey: String!) {
    eventLogs {
        FreeConcurrencySlotForStep(runId: $runId, stepKey: $stepKey) {
            success
        }
    }
}
"""
