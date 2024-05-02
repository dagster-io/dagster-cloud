from dagster._core.libraries import DagsterLibraryRegistry

from .instance import DagsterCloudAgentInstance as DagsterCloudAgentInstance
from .storage.compute_logs import CloudComputeLogManager as CloudComputeLogManager
from .version import __version__

DagsterLibraryRegistry.register("dagster-cloud", __version__)
