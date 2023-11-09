import os
from typing import Any, Dict, Generator, List

import dagster._check as check
from dagster import AssetObservation, OpExecutionContext
from dagster._annotations import experimental
from gql import Client, gql
from gql.transport.exceptions import TransportError
from gql.transport.requests import RequestsHTTPTransport

from ..dagster_insights.errors import DagsterInsightsError
from ..dagster_insights.metrics_utils import (
    PUT_CLOUD_METRICS_MUTATION,
    DagsterMetric as DagsterMetric,
    put_metrics as put_metrics,
)
from ..instance import DagsterCloudAgentInstance


def _chunks(chunk_list: List[Any], length: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(chunk_list), length):
        yield chunk_list[i : i + length]


@experimental
def put_context_metrics(
    context: OpExecutionContext,
    metrics: List[DagsterMetric],
) -> None:
    """Experimental: Store metrics in the dagster cloud metrics store. This method is useful when you would like to
    store run, asset or asset group materialization metric data to view in the insights UI.

    Currently only supported in Dagster Cloud

    Args:
        context (OpExecutionContext): the execution context used for asset materialization
        metrics (List[DagsterMetric]): metrics to store in the dagster metrics store
    """
    check.list_param(metrics, "metrics", of_type=DagsterMetric)
    check.inst_param(context, "context", OpExecutionContext)
    if not isinstance(context.instance, DagsterCloudAgentInstance):
        context.log.info("Dagster instance is not a DagsterCloudAgentInstance, skipping metrics")
        return

    transport = RequestsHTTPTransport(
        url=os.getenv("DAGSTER_METRICS_DAGIT_URL", f"{context.instance.dagit_url}graphql"),
        use_json=True,
        timeout=300,
        headers={"Dagster-Cloud-Api-Token": context.instance.dagster_cloud_agent_token},
    )
    client = Client(transport=transport, fetch_schema_from_transport=True)

    metric_graphql_inputs: List[Dict[str, Any]] = []

    if context.dagster_run.external_job_origin is None:
        raise DagsterInsightsError("dagster run for this context has not started yet")

    if context.has_assets_def:
        for selected_asset_keys in _chunks(list(context.selected_asset_keys), 5):
            metric_graphql_inputs.append(
                {
                    "runId": context.run_id,
                    "stepKey": context.get_step_execution_context().step.key,
                    "codeLocationName": context.dagster_run.external_job_origin.location_name,
                    "repositoryName": (
                        context.dagster_run.external_job_origin.external_repository_origin.repository_name
                    ),
                    "assetMetricDefinitions": [
                        {
                            "assetKey": selected_asset_key.to_python_identifier(),
                            "assetGroup": context.assets_def.group_names_by_key.get(
                                selected_asset_key, None
                            ),
                            "partition": (
                                context.partition_key if context.has_partition_key else None
                            ),
                            "metricValues": [
                                {
                                    "metricValue": metric_def.metric_value,
                                    "metricName": metric_def.metric_name,
                                }
                                for metric_def in metrics
                            ],
                        }
                        for selected_asset_key in selected_asset_keys
                    ],
                }
            )
    else:
        metric_graphql_inputs.append(
            {
                "runId": context.run_id,
                "stepKey": context.get_step_execution_context().step.key,
                "codeLocationName": context.dagster_run.external_job_origin.location_name,
                "repositoryName": (
                    context.dagster_run.external_job_origin.external_repository_origin.repository_name
                ),
                "jobMetricDefinitions": [
                    {
                        "metricValues": [
                            {
                                "metricValue": metric_def.metric_value,
                                "metricName": metric_def.metric_name,
                            }
                            for metric_def in metrics
                        ],
                    }
                ],
            }
        )
    for metric_graphql_inputs in _chunks(metric_graphql_inputs, 5):
        try:
            result = client.execute(
                gql(PUT_CLOUD_METRICS_MUTATION),
                variable_values={"metrics": metric_graphql_inputs},
            )
            if (
                result.get("createOrUpdateMetrics", {}).get("__typename")
                != "CreateOrUpdateMetricsSuccess"
            ):
                context.log.error(
                    "Failed to store metrics with error"
                    f" {result.get('createOrUpdateMetrics', {}).get('message')}"
                )
        except (TransportError, TypeError, KeyError) as exc:
            context.log.error(f"Failed to store metrics with error {exc}")
            return

    context.log.info("Successfully stored metrics")


@experimental
def store_dbt_adapter_metrics(
    context: OpExecutionContext,
    manifest: Dict[Any, Any],
    run_results: Dict[Any, Any],
) -> Generator[AssetObservation, Any, Any]:
    """Experimental: Store dbt adapter response metrics in the dagster cloud metrics store. This method is useful when you would like to store dbt adapter response metrics to view in the insights UI.

    Currently only supported in Dagster Cloud

    Args:
        context (OpExecutionContext): the execution context used for asset materialization
        manifest (Dict[Any, Any]): the manifest of the dbt command
        run_results (Dict[Any, Any]): the run results of the dbt command
    """
    check.inst_param(context, "context", OpExecutionContext)
    check.dict_param(manifest, "manifest")
    check.dict_param(run_results, "run_results")

    if not isinstance(context.instance, DagsterCloudAgentInstance):
        context.log.info("Dagster instance is not a DagsterCloudAgentInstance, skipping metrics")
        return

    transport = RequestsHTTPTransport(
        url=os.getenv("DAGSTER_METRICS_DAGIT_URL", f"{context.instance.dagit_url}graphql"),
        use_json=True,
        timeout=300,
        headers={"Dagster-Cloud-Api-Token": context.instance.dagster_cloud_agent_token},
    )
    client = Client(transport=transport, fetch_schema_from_transport=True)

    if context.dagster_run.external_job_origin is None:
        raise DagsterInsightsError("dagster run for this context has not started yet")
    assetMetricDefinitions = []
    for result in run_results["results"]:
        node = manifest["nodes"][result["unique_id"]]
        metric_values = []
        assetKey = next(
            iter(
                filter(
                    lambda asset_key: asset_key.path[-1] == node["name"],
                    context.selected_asset_keys,
                )
            ),
            None,
        )

        if not assetKey:
            continue

        for adapter_response_key in result["adapter_response"]:
            if adapter_response_key in ["_message", "code"]:
                continue
            if adapter_response_key == "query_id":
                yield AssetObservation(
                    asset_key=assetKey,
                    metadata={"query_id": result["adapter_response"][adapter_response_key]},
                )
            if isinstance(result["adapter_response"][adapter_response_key], float) or isinstance(
                result["adapter_response"][adapter_response_key], int
            ):
                metric_values.append(
                    {
                        "metricValue": result["adapter_response"][adapter_response_key],
                        "metricName": adapter_response_key,
                    }
                )
        partition = None
        try:
            partition = context.asset_partition_key_for_output(assetKey)
        except check.CheckError:
            pass

        assetMetricDefinitions.append(
            {
                "assetKey": assetKey.to_python_identifier(),
                "assetGroup": context.assets_def.group_names_by_key.get(assetKey, None),
                "partition": partition,
                "metricValues": metric_values,
            }
        )
    metric_graphql_inputs = []
    for assetMetricDefinitions in _chunks(assetMetricDefinitions, 5):
        metric_graphql_inputs.append(
            {
                "runId": context.run_id,
                "stepKey": context.get_step_execution_context().step.key,
                "codeLocationName": context.dagster_run.external_job_origin.location_name,
                "repositoryName": (
                    context.dagster_run.external_job_origin.external_repository_origin.repository_name
                ),
                "assetMetricDefinitions": assetMetricDefinitions,
            }
        )
    for metric_graphql_inputs in _chunks(metric_graphql_inputs, 5):
        try:
            result = client.execute(
                gql(PUT_CLOUD_METRICS_MUTATION),
                variable_values={"metrics": metric_graphql_inputs},
            )
            if (
                result.get("createOrUpdateMetrics", {}).get("__typename")
                != "CreateOrUpdateMetricsSuccess"
            ):
                context.log.error(
                    "Failed to store metrics with error"
                    f" {result.get('createOrUpdateMetrics', {}).get('message')}"
                )
        except (TransportError, TypeError, KeyError) as exc:
            context.log.error(f"Failed to store metrics with error {exc}")
            return
    context.log.info("Successfully stored metrics")
