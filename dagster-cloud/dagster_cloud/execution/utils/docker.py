from typing import Optional, Tuple

from dagster_cloud.execution.utils import TaskStatus


def check_on_container(client, container_name_or_id: str) -> Tuple[TaskStatus, Optional[str]]:
    try:
        container = client.containers.get(container_name_or_id)
    except Exception as e:
        return TaskStatus.NOT_FOUND, f"Hit Exception trying to get the container: {e}"

    if container.status == "running":
        return TaskStatus.RUNNING, None

    try:
        container_info = container.wait(timeout=0.1)
    except Exception as e:
        return (
            TaskStatus.UNKNOWN,
            f"Container status is {container.status}. Hit exception attempting to get its return code: {e}",
        )
    else:
        ret_code = container_info.get("StatusCode")
        if ret_code == 0:
            return TaskStatus.SUCCESS, None
        elif ret_code:
            return (
                TaskStatus.FAILURE,
                f"Container status is {container.status}. Return code is {str(ret_code)}.",
            )

    return TaskStatus.UNKNOWN, f"Container status is {container.status}. Return code is unknown."
