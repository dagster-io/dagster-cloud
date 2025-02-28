import logging

from dagster import DagsterRun


def extract_run_attributes(deployment_name, run: DagsterRun) -> dict[str, str]:
    attributes = {
        "job": run.job_name or "unknown",
        "repository": "unknown",
        "location": "unknown",
        "deployment": deployment_name,
    }

    try:
        if run.remote_job_origin:
            attributes["repository"] = (
                run.remote_job_origin.repository_origin.repository_name or "unknown"
            )
            attributes["location"] = run.remote_job_origin.location_name or "unknown"
    except Exception as e:  # pylint: disable=broad-except
        logging.warning("Failed to extract additional run attributes", exc_info=e)
    finally:
        return attributes
