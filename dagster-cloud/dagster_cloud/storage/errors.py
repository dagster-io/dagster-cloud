import dagster._check as check


class GraphQLStorageError(Exception):
    """Raise this when there's an error in the GraphQL layer"""


DEFAULT_MAINTENANCE_TIMEOUT = 3600 * 4 * 4
DEFAULT_MAINTENANCE_RETRY_INTERVAL = 30


class DagsterCloudMaintenanceException(Exception):
    def __init__(self, message, timeout, retry_interval):
        self.timeout = check.opt_numeric_param(timeout, "timeout", DEFAULT_MAINTENANCE_TIMEOUT)
        self.retry_interval = check.opt_numeric_param(
            retry_interval, "retry_interval", DEFAULT_MAINTENANCE_RETRY_INTERVAL
        )
        super(DagsterCloudMaintenanceException, self).__init__(message)
