import logging
import sys
import threading
from collections import namedtuple
from enum import Enum
from typing import Dict, List, NamedTuple, Optional, Set

import dagster._check as check
from dagster import DagsterInstance
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.launcher import CheckRunHealthResult, WorkerStatus
from dagster._core.storage.pipeline_run import IN_PROGRESS_RUN_STATUSES, PipelineRunsFilter
from dagster._serdes import whitelist_for_serdes
from dagster._utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info
from dagster_cloud.instance import DagsterCloudAgentInstance, InstanceRef
from dagster_cloud.storage.runs import GraphQLRunStorage
from dagster_cloud.util import SERVER_HANDLE_TAG, is_isolated_run


@whitelist_for_serdes
class CloudCodeServerStatus(Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"


@whitelist_for_serdes
class CloudCodeServerHeartbeat(
    NamedTuple(
        "_CloudRunWorkerStatus",
        [
            ("location_name", str),
            ("server_status", CloudCodeServerStatus),
            ("error", Optional[SerializableErrorInfo]),
        ],
    )
):
    def __new__(
        cls,
        location_name: str,
        server_status: CloudCodeServerStatus,
        error: Optional[SerializableErrorInfo] = None,
    ):
        return super(CloudCodeServerHeartbeat, cls).__new__(
            cls,
            location_name=check.str_param(location_name, "location_name"),
            server_status=check.inst_param(server_status, "server_status", CloudCodeServerStatus),
            error=check.opt_inst_param(error, "error", SerializableErrorInfo),
        )


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


def get_cloud_run_worker_statuses(instance, deployment_names, logger):

    statuses = {}

    # protected with a lock inside the method
    active_grpc_server_handles = [
        str(s) for s in instance.user_code_launcher.get_active_grpc_server_handles()
    ]

    for deployment_name in deployment_names:
        with DagsterInstance.from_ref(
            instance.ref_for_deployment(deployment_name)
        ) as scoped_instance:
            runs = scoped_instance.get_runs(PipelineRunsFilter(statuses=IN_PROGRESS_RUN_STATUSES))
            statuses_for_deployment = []
            for run in runs:
                if is_isolated_run(run):
                    launcher = scoped_instance.run_launcher
                    statuses_for_deployment.append(
                        CloudRunWorkerStatus.from_check_run_health_result(
                            run.run_id, launcher.check_run_worker_health(run)
                        )
                    )
                else:
                    if scoped_instance.agent_replicas_enabled:
                        # Not currently supported for non isolated run monitoring
                        continue

                    server_handle_for_run = run.tags.get(SERVER_HANDLE_TAG)
                    if (
                        server_handle_for_run
                        and server_handle_for_run not in active_grpc_server_handles
                    ):
                        logger.info(
                            f"Detected failure: run {run.run_id} on server {server_handle_for_run} is not in the active server handles {', '.join(active_grpc_server_handles)}"
                        )
                        statuses_for_deployment.append(
                            CloudRunWorkerStatus(
                                run.run_id,
                                WorkerStatus.FAILED,
                                "The gRPC server that was hosting this run is no longer running.",
                            )
                        )

            statuses[deployment_name] = statuses_for_deployment

    return statuses


def run_worker_monitoring_thread(
    instance: DagsterCloudAgentInstance,
    deployments_to_check: Set[str],
    statuses_dict: Dict[str, List[CloudRunWorkerStatus]],
    run_worker_monitoring_lock: threading.Lock,
    shutdown_event: threading.Event,
):
    logger = logging.getLogger("dagster_cloud")
    check.inst_param(instance, "instance", DagsterCloudAgentInstance)
    logger.debug("Run Monitor thread has started")
    while not shutdown_event.is_set():
        run_worker_monitoring_thread_iteration(
            instance, deployments_to_check, statuses_dict, run_worker_monitoring_lock, logger
        )
        shutdown_event.wait(instance.dagster_cloud_run_worker_monitoring_interval_seconds)
    logger.debug("Run monitor thread shutting down")


def run_worker_monitoring_thread_iteration(
    instance: DagsterCloudAgentInstance,
    deployments_to_check: Set[str],
    statuses_dict: Dict[str, List[CloudRunWorkerStatus]],
    run_worker_monitoring_lock: threading.Lock,
    logger: logging.Logger,
):
    try:
        # Check the deployment names
        with run_worker_monitoring_lock:
            deployments_to_check_copy = deployments_to_check.copy()

        statuses = get_cloud_run_worker_statuses(instance, deployments_to_check_copy, logger)
        logger.debug("Thread got statuses: {}".format(statuses))
        with run_worker_monitoring_lock:
            statuses_dict.clear()
            for deployment_name, statuses in statuses.items():
                statuses_dict[deployment_name] = statuses

    except Exception:
        error_info = serializable_error_info_from_exc_info(sys.exc_info())
        logger.error("Caught error in run monitoring thread:\n{}".format(error_info))


def start_run_worker_monitoring_thread(
    instance: DagsterCloudAgentInstance,
    deployments_to_check: Set[str],
    statuses_dict: Dict[str, List[CloudRunWorkerStatus]],
    run_worker_monitoring_lock: threading.Lock,
):
    shutdown_event = threading.Event()
    thread = threading.Thread(
        target=run_worker_monitoring_thread,
        args=(
            instance,
            deployments_to_check,
            statuses_dict,
            run_worker_monitoring_lock,
            shutdown_event,
        ),
        name="run-monitoring",
    )
    thread.start()
    return thread, shutdown_event
