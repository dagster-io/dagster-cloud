import logging
import sys
import threading
from enum import Enum
from typing import Dict, List, NamedTuple, Optional, Sequence, Set, Union

import dagster._check as check
import grpc
from dagster import DagsterInstance, DagsterRunStatus
from dagster._core.launcher import CheckRunHealthResult, WorkerStatus
from dagster._core.storage.pipeline_run import IN_PROGRESS_RUN_STATUSES, PipelineRunsFilter
from dagster._serdes import whitelist_for_serdes
from dagster._utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info

from dagster_cloud.instance import DagsterCloudAgentInstance
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


class _GetCurrentRunsError(Enum):
    UNIMPLEMENTED = "UNIMPLEMENTED"
    OTHER_ERROR = "OTHER_ERROR"


def _is_grpc_unimplemented_error(error: Exception) -> bool:
    cause = error.__cause__
    if not isinstance(cause, grpc.RpcError):
        return False
    return cause.code() == grpc.StatusCode.UNIMPLEMENTED


def _is_grpc_unknown_error(error: Exception) -> bool:
    cause = error.__cause__
    if not isinstance(cause, grpc.RpcError):
        return False
    return cause.code() == grpc.StatusCode.UNKNOWN


def get_cloud_run_worker_statuses(instance: DagsterCloudAgentInstance, deployment_names, logger):
    statuses = {}

    # protected with a lock inside the method
    active_grpc_server_handles = instance.user_code_launcher.get_active_grpc_server_handles()
    active_grpc_server_handle_strings = [str(s) for s in active_grpc_server_handles]

    active_non_isolated_run_ids_by_server_handle: Dict[
        str, Union[Sequence[str], _GetCurrentRunsError]
    ] = {}
    if instance.user_code_launcher.supports_get_current_runs_for_server_handle:
        for handle in active_grpc_server_handles:
            try:
                run_ids = instance.user_code_launcher.get_current_runs_for_server_handle(handle)
                active_non_isolated_run_ids_by_server_handle[str(handle)] = run_ids
            except Exception as e:
                logger.exception(
                    "Run monitoring: hit error with GetCurrentRunsResult for handle: {}".format(
                        handle
                    )
                )
                if _is_grpc_unimplemented_error(e):
                    logger.info(
                        "Run monitoring: get_current_runs not implemented, skipping server handle"
                    )
                    active_non_isolated_run_ids_by_server_handle[
                        str(handle)
                    ] = _GetCurrentRunsError.UNIMPLEMENTED

                # NOTE: multipex servers on version 1.1.4 and 1.1.5 had a bug where they would return
                # UNKNOWN errors for GetCurrentRuns. For backcompat, ignore it as unimplemented
                elif _is_grpc_unknown_error(e):
                    logger.info(
                        "Run monitoring: get_current_runs returned UNKNOWN error, skipping server"
                        " handle"
                    )
                    active_non_isolated_run_ids_by_server_handle[
                        str(handle)
                    ] = _GetCurrentRunsError.UNIMPLEMENTED
                else:
                    logger.info("Run monitoring: error getting current runs for server handle")
                    active_non_isolated_run_ids_by_server_handle[
                        str(handle)
                    ] = _GetCurrentRunsError.OTHER_ERROR

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

                    if run.status != DagsterRunStatus.STARTED:
                        # Rely on timeout for runs in STARTING
                        continue

                    server_handle_for_run = run.tags.get(SERVER_HANDLE_TAG)

                    if not server_handle_for_run:
                        # shouldn't be able to happen except right when a user upgrades their agent
                        # and has old runs still in progress
                        continue

                    if server_handle_for_run not in active_grpc_server_handle_strings:
                        logger.info(
                            f"Detected failure: run {run.run_id} on server"
                            f" {server_handle_for_run} is not in the active server handles"
                            f" {', '.join(active_grpc_server_handles)}"
                        )
                        statuses_for_deployment.append(
                            CloudRunWorkerStatus(
                                run.run_id,
                                WorkerStatus.FAILED,
                                (
                                    "The code location server that was hosting this run is no"
                                    " longer running. Upgrading to a newer version of dagster in"
                                    " your asset/job code (version 1.1.4) may prevent this from"
                                    " occuring."
                                ),
                            )
                        )
                        continue

                    get_runs_result_for_server_handle = (
                        active_non_isolated_run_ids_by_server_handle[server_handle_for_run]
                    )
                    if get_runs_result_for_server_handle == _GetCurrentRunsError.UNIMPLEMENTED:
                        # Server is an older version than 1.1.4- we can't fully check on the run
                        continue

                    if get_runs_result_for_server_handle == _GetCurrentRunsError.OTHER_ERROR:
                        logger.info(
                            f"Detected failure: run {run.run_id} on server {server_handle_for_run}."
                            " Server is not responding"
                        )
                        statuses_for_deployment.append(
                            CloudRunWorkerStatus(
                                run.run_id,
                                WorkerStatus.FAILED,
                                (
                                    "The code location server that was hosting this run is not"
                                    " responding. It may have crashed or been OOM killed."
                                ),
                            )
                        )
                        continue

                    if not isinstance(get_runs_result_for_server_handle, list):
                        check.failed(
                            (
                                "get_runs_result_for_server_handle is an unexpected type:"
                                f" {get_runs_result_for_server_handle}"
                            ),
                        )

                    # If the run is not in the list of runs returned by the server, maybe the server
                    # crashed and ECS/etc. restarted it
                    if run.run_id not in get_runs_result_for_server_handle:
                        logger.info(
                            f"Detected failure: run {run.run_id} on server"
                            f" {server_handle_for_run} is not in the current runs"
                            f" {', '.join(get_runs_result_for_server_handle)}"
                        )
                        statuses_for_deployment.append(
                            CloudRunWorkerStatus(
                                run.run_id,
                                WorkerStatus.FAILED,
                                (
                                    "The run process can't be found on the code location server."
                                    " The server may have crashed or been OOM killed."
                                ),
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
