import logging
import sys
import threading
from collections import namedtuple
from typing import List, NamedTuple, Optional

import dagster._check as check
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.launcher import CheckRunHealthResult, WorkerStatus
from dagster.core.storage.pipeline_run import IN_PROGRESS_RUN_STATUSES, PipelineRunsFilter
from dagster.serdes import whitelist_for_serdes
from dagster.utils.error import serializable_error_info_from_exc_info
from dagster_cloud.instance import DagsterCloudAgentInstance, InstanceRef


@whitelist_for_serdes
class CloudRunWorkerStatus(
    NamedTuple(
        "_CloudRunWorkerStatus",
        [("run_id", str), ("status_type", WorkerStatus), ("message", Optional[str])],
    )
):
    def __new__(cls, run_id: str, status_type: WorkerStatus, message: Optional[str] = None):
        return super(CloudRunWorkerStatus, cls).__new__(
            cls,
            run_id=check.str_param(run_id, "run_id"),
            status_type=check.inst_param(status_type, "status_type", WorkerStatus),
            message=check.opt_str_param(message, "message"),
        )

    @classmethod
    def from_check_run_health_result(cls, run_id: str, result: CheckRunHealthResult):
        check.inst_param(result, "result", CheckRunHealthResult)
        return CloudRunWorkerStatus(run_id, result.status, result.msg)


@whitelist_for_serdes
class CloudRunWorkerStatuses(
    NamedTuple(
        "_CloudRunWorkerStatuses",
        [
            ("statuses", List[CloudRunWorkerStatus]),
            ("run_worker_monitoring_supported", bool),
            ("run_worker_monitoring_thread_alive", Optional[bool]),
        ],
    )
):
    def __new__(
        cls,
        statuses: Optional[List[CloudRunWorkerStatus]],
        run_worker_monitoring_supported: bool,
        run_worker_monitoring_thread_alive: Optional[bool],
    ):
        return super(CloudRunWorkerStatuses, cls).__new__(
            cls,
            statuses=check.opt_list_param(statuses, "statuses", of_type=CloudRunWorkerStatus),
            run_worker_monitoring_supported=check.bool_param(
                run_worker_monitoring_supported, "run_worker_monitoring_supported"
            ),
            run_worker_monitoring_thread_alive=check.opt_bool_param(
                run_worker_monitoring_thread_alive, "run_worker_monitoring_thread_alive"
            ),
        )


def get_cloud_run_worker_statuses(instance):
    launcher = instance.run_launcher
    check.invariant(launcher.supports_check_run_worker_health)
    runs = instance.get_runs(PipelineRunsFilter(statuses=IN_PROGRESS_RUN_STATUSES))
    return [
        CloudRunWorkerStatus.from_check_run_health_result(
            run.run_id, launcher.check_run_worker_health(run)
        )
        for run in runs
    ]


def run_worker_monitoring_thread(
    instance_ref: InstanceRef,
    statuses_list: List[CloudRunWorkerStatus],
    statuses_lock: threading.Lock,
    shutdown_event: threading.Event,
):
    logger = logging.getLogger("dagster_cloud")
    check.inst_param(instance_ref, "instance_ref", InstanceRef)
    logger.debug("Run Monitor thread has started")
    with DagsterCloudAgentInstance.from_ref(instance_ref) as instance:
        while not shutdown_event.is_set():
            run_worker_monitoring_thread_iteration(instance, statuses_list, statuses_lock, logger)
            shutdown_event.wait(instance.dagster_cloud_run_worker_monitoring_interval_seconds)
    logger.debug("Run monitor thread shutting down")


def run_worker_monitoring_thread_iteration(
    instance: DagsterCloudAgentInstance,
    statuses_list: List[CloudRunWorkerStatus],
    statuses_lock: threading.Lock,
    logger: logging.Logger,
):
    try:
        statuses = get_cloud_run_worker_statuses(instance)
        logger.debug("Thread got statuses: {}".format(statuses))
        with statuses_lock:
            statuses_list.clear()
            statuses_list.extend(statuses)
    except Exception:
        error_info = serializable_error_info_from_exc_info(sys.exc_info())
        logger.error("Caught error in run monitoring thread:\n{}".format(error_info))


def start_run_worker_monitoring_thread(
    instance: DagsterCloudAgentInstance,
    statuses_list: List[CloudRunWorkerStatus],
    statuses_lock: threading.Lock,
):
    shutdown_event = threading.Event()
    thread = threading.Thread(
        target=run_worker_monitoring_thread,
        args=(instance.get_ref(), statuses_list, statuses_lock, shutdown_event),
        name="run-monitoring",
    )
    thread.start()
    return thread, shutdown_event
