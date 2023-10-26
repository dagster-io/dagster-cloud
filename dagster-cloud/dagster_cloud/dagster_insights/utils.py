from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import dagster._check as check
from dagster import AssetExecutionContext, DagsterInstance, OpExecutionContext
from dagster._annotations import experimental
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from ..instance import DagsterCloudAgentInstance
from .query import PUT_CLOUD_METRICS_MUTATION


def _chunks(chunk_list: List[Any], length: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(chunk_list), length):
        yield chunk_list[i : i + length]


@experimental
class DagsterMetric(NamedTuple):
    """Experimental: This class gives information about a Metric.

    Args:
        metric_name (str): name of the metric
        metric_value (float): value of the metric
    """

    metric_name: str
    metric_value: float


def query_graphql_from_instance(
    instance: DagsterInstance, query_text: str, variables=None
) -> Dict[str, Any]:
    headers = {}

    url, cloud_token = get_url_and_token_from_instance(instance)

    headers["Dagster-Cloud-API-Token"] = cloud_token

    transport = RequestsHTTPTransport(
        url=url,
        use_json=True,
        timeout=300,
        headers={"Dagster-Cloud-Api-Token": cloud_token},
    )
    client = Client(transport=transport, fetch_schema_from_transport=True)
    return client.execute(gql(query_text), variable_values=variables or dict())


def get_url_and_token_from_instance(instance: DagsterInstance) -> Tuple[str, str]:
    if not isinstance(instance, DagsterCloudAgentInstance):
        raise RuntimeError("This asset only functions in a running Dagster Cloud instance")

    return f"{instance.dagit_url}graphql", instance.dagster_cloud_agent_token


@experimental
def put_metrics(
    context: Union[OpExecutionContext, AssetExecutionContext],
    run_id: str,
    step_key: str,
    metrics: List[DagsterMetric],
    asset_key: Optional[str] = None,
    partition: Optional[str] = None,
) -> Dict[str, Any]:
    """Experimental: Store metrics in the dagster cloud metrics store. This method is useful when you would like to
    store run, asset or asset group materialization metric data to view in the insights UI.

    To associate a metric with an asset, you must provide the asset_key and partition arguments. If you do not provide
    these arguments, the metric will be associated with the job. Often you will want to store both job and asset
    metrics, in which case you should call this method twice, once with the asset_key and partition arguments, and
    once without.

    Currently only supported in Dagster Cloud.

    Args:
        context (Union[OpExecutionContext, AssetExecutionContext]): the execution context used to query Cloud
        run_id (str): id of the dagster run
        step_key (str): key of the step
        code_location_name (str): name of the code location
        repository_name (str): name of the repository
        metrics (List[DagsterMetric]): metrics to store in the dagster metrics store
        asset_key (Optional[str]): key of the asset, if storing an asset metric
        partition (Optional[str]): partition of the asset, if storing an asset metric
    """
    check.list_param(metrics, "metrics", of_type=DagsterMetric)
    check.str_param(run_id, "run_id", run_id)
    check.str_param(step_key, "step_key", step_key)
    check.opt_str_param(asset_key, "asset_key", asset_key)
    check.opt_str_param(partition, "partition", partition)

    url, token = get_url_and_token_from_instance(context.instance)

    transport = RequestsHTTPTransport(
        url=url,
        use_json=True,
        timeout=300,
        headers={"Dagster-Cloud-Api-Token": token},
    )
    Client(transport=transport, fetch_schema_from_transport=True)

    metric_graphql_input = {
        "runId": run_id,
        "stepKey": step_key,
        "assetMetricDefinitions": [],
        "jobMetricDefinitions": [],
    }

    if asset_key is not None:
        metric_graphql_input["assetMetricDefinitions"].append(
            {
                "assetKey": asset_key,
                "partition": partition,
                "metricValues": [
                    {
                        "metricValue": metric_def.metric_value,
                        "metricName": metric_def.metric_name,
                    }
                    for metric_def in metrics
                ],
            }
        )
    else:
        metric_graphql_input["jobMetricDefinitions"].append(
            {
                "metricValues": [
                    {
                        "metricValue": metric_def.metric_value,
                        "metricName": metric_def.metric_name,
                    }
                    for metric_def in metrics
                ],
            }
        )

    return query_graphql_from_instance(
        context.instance,
        PUT_CLOUD_METRICS_MUTATION,
        variables={"metrics": [metric_graphql_input]},
    )
