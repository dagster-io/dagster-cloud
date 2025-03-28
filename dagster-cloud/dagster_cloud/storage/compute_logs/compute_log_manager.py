from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional

import requests
from dagster import (
    Field,
    IntSource,
    StringSource,
    _check as check,
)
from dagster._core.storage.cloud_storage_compute_log_manager import CloudStorageComputeLogManager
from dagster._core.storage.compute_log_manager import ComputeIOType
from dagster._core.storage.local_compute_log_manager import (
    IO_TYPE_EXTENSION,
    LocalComputeLogManager,
)
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils import ensure_file
from dagster_cloud_cli.core.errors import raise_http_error
from dagster_cloud_cli.core.headers.auth import DagsterCloudInstanceScope
from dagster_shared import seven
from requests.adapters import HTTPAdapter
from typing_extensions import Self

if TYPE_CHECKING:
    from dagster_cloud.instance import DagsterCloudAgentInstance  # noqa: F401


class CloudComputeLogManager(
    CloudStorageComputeLogManager["DagsterCloudAgentInstance"], ConfigurableClass
):
    def __init__(
        self,
        local_dir=None,
        upload_interval=None,
        inst_data=None,
    ):
        # proxy calls to local compute log manager (for subscriptions, etc)
        if not local_dir:
            local_dir = seven.get_system_temp_directory()

        self._upload_session = requests.Session()
        adapter = HTTPAdapter(max_retries=3)
        self._upload_session.mount("http://", adapter)
        self._upload_session.mount("https://", adapter)

        self._local_manager = LocalComputeLogManager(local_dir)
        self._upload_interval = check.opt_int_param(upload_interval, "upload_interval")
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "local_dir": Field(StringSource, is_required=False),
            "upload_interval": Field(IntSource, is_required=False),
        }

    @classmethod
    def from_config_value(cls, inst_data: ConfigurableClassData, config_value: Any) -> Self:
        return cls(inst_data=inst_data, **config_value)

    @property
    def local_manager(self) -> LocalComputeLogManager:
        return self._local_manager

    @property
    def upload_interval(self) -> Optional[int]:
        return self._upload_interval

    def delete_logs(
        self, log_key: Optional[Sequence[str]] = None, prefix: Optional[Sequence[str]] = None
    ):
        raise NotImplementedError("User Agent should not need to delete compute logs")

    def download_url_for_type(self, log_key: Sequence[str], io_type: ComputeIOType):
        raise NotImplementedError("User Agent should not need to download compute logs")

    def display_path_for_type(self, log_key: Sequence[str], io_type: ComputeIOType):
        raise NotImplementedError("User Agent should not need to download compute logs")

    def cloud_storage_has_logs(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> bool:
        """Returns whether the cloud storage contains logs for a given log key."""
        return False

    def upload_to_cloud_storage(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial=False
    ):
        path = self.local_manager.get_captured_local_path(log_key, IO_TYPE_EXTENSION[io_type])
        ensure_file(path)
        params: dict[str, Any] = {
            "log_key": log_key,
            "io_type": io_type.value,
            # for back-compat
            "run_id": log_key[0],
            "key": log_key[-1],
        }
        if partial:
            params["partial"] = True
        resp = self._instance.requests_managed_retries_session.get(
            self._instance.dagster_cloud_gen_logs_url_url,
            params=params,
            headers=self._instance.dagster_cloud_api_headers(DagsterCloudInstanceScope.DEPLOYMENT),
            timeout=self._instance.dagster_cloud_api_timeout,
            proxies=self._instance.dagster_cloud_api_proxies,
        )
        raise_http_error(resp)
        resp_data = resp.json()

        if resp_data.get("skip_upload"):
            return

        with open(path, "rb") as f:
            self._upload_session.post(
                resp_data["url"],
                data=resp_data["fields"],
                files={"file": f},
            )

    def download_from_cloud_storage(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial=False
    ):
        raise NotImplementedError("User Agent should not need to download compute logs")

    def on_subscribe(self, subscription):
        raise NotImplementedError("User Agent should not need to download compute logs")
