import os
import tempfile
import zlib
from contextlib import contextmanager

import dagster.seven as seven
from dagster import Field, StringSource
from dagster import _check as check
from dagster.core.storage.compute_log_manager import (
    MAX_BYTES_FILE_READ,
    ComputeIOType,
    ComputeLogManager,
)
from dagster.core.storage.local_compute_log_manager import LocalComputeLogManager
from dagster.serdes import ConfigurableClass, ConfigurableClassData
from dagster.utils import ensure_file

from ...errors import raise_http_error


class CloudComputeLogManager(ComputeLogManager, ConfigurableClass):
    def __init__(
        self,
        local_dir=None,
        inst_data=None,
    ):

        # proxy calls to local compute log manager (for subscriptions, etc)
        if not local_dir:
            local_dir = seven.get_system_temp_directory()

        self.local_manager = LocalComputeLogManager(local_dir)
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "local_dir": Field(StringSource, is_required=False),
        }

    @staticmethod
    def from_config_value(inst_data, config_value):
        return CloudComputeLogManager(inst_data=inst_data, **config_value)

    @contextmanager
    def _watch_logs(self, pipeline_run, step_key=None):
        # proxy watching to the local compute log manager, interacting with the filesystem
        with self.local_manager._watch_logs(  # pylint: disable=protected-access
            pipeline_run, step_key
        ):
            yield

    def get_local_path(self, run_id, key, io_type):
        return self.local_manager.get_local_path(run_id, key, io_type)

    def on_watch_start(self, pipeline_run, step_key):
        self.local_manager.on_watch_start(pipeline_run, step_key)

    def is_watch_completed(self, run_id, key):
        return self.local_manager.is_watch_completed(run_id, key)

    def on_watch_finish(self, pipeline_run, step_key):
        self.local_manager.on_watch_finish(pipeline_run, step_key)
        key = self.local_manager.get_key(pipeline_run, step_key)
        self._upload_from_local(pipeline_run.run_id, key, ComputeIOType.STDOUT)
        self._upload_from_local(pipeline_run.run_id, key, ComputeIOType.STDERR)

    def _upload_from_local(self, run_id, key, io_type):
        path = self.get_local_path(run_id, key, io_type)
        ensure_file(path)
        with open(path, "rb") as data:
            with tempfile.TemporaryDirectory() as temp_dir:
                dst = os.path.join(temp_dir, "compute_log.tmp")
                with open(dst, "wb") as compressed:
                    compressed.write(zlib.compress(data.read()))

                with open(dst, "rb") as compressed:
                    resp = self._instance.requests_session.post(
                        self._instance.dagster_cloud_upload_logs_url,
                        headers=self._instance.dagster_cloud_api_headers,
                        params={
                            "run_id": run_id,
                            "key": key,
                            "io_type": io_type.value,
                            "compressed": True,
                        },
                        files={"compute_log.tmp": compressed},
                        timeout=self._instance.dagster_cloud_api_timeout,
                    )
        raise_http_error(resp)

    def download_url(self, run_id, key, io_type):
        raise NotImplementedError("User Agent should not need to download compute logs")

    def read_logs_file(self, run_id, key, io_type, cursor=0, max_bytes=MAX_BYTES_FILE_READ):
        raise NotImplementedError("User Agent should not need to download compute logs")

    def on_subscribe(self, subscription):
        raise NotImplementedError("User Agent should not need to download compute logs")
