from abc import abstractmethod

from dagster.core.launcher import RunLauncher


class WatchfulRunLauncher(RunLauncher):
    @abstractmethod
    def check_run_health(self, run_id):
        """
        Checks the health of a run.

        Args:
            run_id (str): The id of the run to check.
        """
