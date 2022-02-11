ALL_STORED_JOB_STATE_QUERY = """
    query jobStates($repositoryOriginId: String, $jobType: InstigationType) {
        schedules {
            jobStates(repositoryOriginId: $repositoryOriginId, jobType: $jobType)
        }
    }
"""


ADD_JOB_STATE_MUTATION = """
    mutation addJobStateMutation($serializedJobState: String!) {
        schedules {
            addJobState(serializedJobState: $serializedJobState) {
                ok
            }
        }
    }
"""

CREATE_JOB_TICK_MUTATION = """
    mutation createJobTickMutation($serializedJobTickData: String!) {
        schedules {
            createJobTick(serializedJobTickData: $serializedJobTickData) {
                tickId
            }
        }
    }
"""

UPDATE_JOB_TICK_MUTATION = """
    mutation updateJobTickMutation($tickId: Int! $serializedJobTickData: String!) {
        schedules {
            updateJobTick(tickId: $tickId, serializedJobTickData: $serializedJobTickData) {
                ok
            }
        }
    }
"""

GET_JOB_TICK_STATS_QUERY = """
    query jobTickStats($jobOriginId: String!) {
        schedules {
            jobTickStats(jobOriginId: $jobOriginId) {
                ticksStarted
                ticksSucceeded
                ticksSkipped
                ticksFailed
            }
        }
    }

"""


GET_JOB_STATE_QUERY = """
    query jobState($jobOriginId: String!) {
        schedules {
            jobState(jobOriginId: $jobOriginId)
        }
    }
"""

GET_JOB_TICKS_QUERY = """
    query jobTicks($jobOriginId: String!, $before: Float, $after: Float, $limit: Int) {
        schedules {
            jobTicks(jobOriginId: $jobOriginId, before: $before, after: $after, limit: $limit)
        }
    }
"""

LATEST_JOB_TICK_QUERY = """
    query latestJobTick($jobOriginId: String!) {
        schedules {
            latestJobTick(jobOriginId: $jobOriginId)
        }
    }
"""

UPDATE_JOB_STATE_MUTATION = """
    mutation updateJobStateMutation($serializedJobState: String!) {
        schedules {
            updateJobState(serializedJobState: $serializedJobState) {
                ok
            }
        }
    }
"""

PURGE_JOB_TICKS_MUTATION = """
    mutation purgeJobTicksMutation($jobOriginId: String!, $tickStatus: InstigationTickStatus, $before: Float) {
        schedules {
            purgeJobTicks(jobOriginId: $jobOriginId, tickStatus: $tickStatus, before: $before) {
                ok
            }
        }
    }

"""
