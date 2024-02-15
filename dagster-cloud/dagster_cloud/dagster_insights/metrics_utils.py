import os
import tempfile
from typing import Any, Dict, Generator, List, NamedTuple, Optional, Tuple, Union

import dagster._check as check
import requests
from dagster import AssetExecutionContext, DagsterInstance, OpExecutionContext
from dagster._annotations import experimental
from dagster_cloud_cli.core.errors import raise_http_error
from dagster_cloud_cli.core.headers.auth import DagsterCloudInstanceScope
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from dagster_cloud.instance import DagsterCloudAgentInstance

from .query import PUT_CLOUD_METRICS_MUTATION, PUT_COST_INFORMATION_MUTATION


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


def _cost_information_chunk_size() -> int:
    return 10000


def chunk_by_opaque_id(
    chunk_list: List[Tuple[str, float]], length: int
) -> Generator[List[Tuple[str, float]], None, None]:
    """Yield chunks of data. Groups by opaque_id so that
    all metrics for a given opaque_id are in the same chunk.
    Chunks will be of size `length` or less unless a single
    opaque_id has more than `length` metrics.
    """
    sorted_chunk_list = sorted(chunk_list, key=lambda x: x[0]) + [("marker", 0.0)]

    current_opaque_id = None
    last_opaque_id_boundary = 0
    last_chunk_boundary = 0
    for i, (opaque_id, _) in enumerate(sorted_chunk_list):
        # If we've seen enough metrics to fill a chunk, yield the chunk minus the
        # current opaque_id, and start a new chunk
        if i - last_chunk_boundary > length:
            yield sorted_chunk_list[last_chunk_boundary:last_opaque_id_boundary]
            last_chunk_boundary = last_opaque_id_boundary

        if opaque_id != current_opaque_id:
            current_opaque_id = opaque_id
            last_opaque_id_boundary = i

    sorted_chunk_list = sorted_chunk_list[:-1]
    if last_chunk_boundary < len(sorted_chunk_list):
        yield sorted_chunk_list[last_chunk_boundary:]


def get_post_request_params(
    instance: DagsterInstance,
) -> Tuple[requests.Session, str, Dict[str, str], int, Optional[Dict[str, str]]]:
    if not isinstance(instance, DagsterCloudAgentInstance):
        raise RuntimeError("This asset only functions in a running Dagster Cloud instance")

    return (
        instance.rest_requests_session,
        instance.dagster_cloud_gen_insights_url_url,
        instance.dagster_cloud_api_headers(DagsterCloudInstanceScope.DEPLOYMENT),
        instance.dagster_cloud_api_timeout,
        instance.dagster_cloud_api_proxies,
    )


def upload_cost_information(
    context: Union[OpExecutionContext, AssetExecutionContext],
    metric_name: str,
    cost_information: List[Tuple[str, float, str]],
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as temp_dir:
        opaque_ids = pa.array([opaque_id for opaque_id, _, _ in cost_information])
        costs = pa.array([cost for _, cost, _ in cost_information])
        query_ids = pa.array([query_id for _, _, query_id in cost_information])
        metric_names = pa.array([metric_name for _, _, _ in cost_information])

        cost_pq_file = os.path.join(temp_dir, "cost.parquet")
        pq.write_table(
            pa.Table.from_arrays(
                [opaque_ids, costs, metric_names, query_ids],
                ["opaque_id", "cost", "metric_name", "query_id"],
            ),
            cost_pq_file,
        )

        instance = context.instance
        session, url, headers, timeout, proxies = get_post_request_params(instance)

        resp = session.post(url, headers=headers, timeout=timeout, proxies=proxies)
        raise_http_error(resp)
        resp_data = resp.json()

        assert "url" in resp_data and "fields" in resp_data, resp_data

        with open(cost_pq_file, "rb") as f:
            session.post(
                resp_data["url"],
                data=resp_data["fields"],
                files={"file": f},
            )


@experimental
def put_cost_information(
    context: Union[OpExecutionContext, AssetExecutionContext],
    metric_name: str,
    cost_information: List[Tuple[str, float, str]],
    start: float,
    end: float,
    submit_gql: bool = True,
) -> None:
    chunk_size = _cost_information_chunk_size()

    try:
        import pyarrow as pyarrow  # pylint: disable=import-error

        try:
            upload_cost_information(context, metric_name, cost_information)
        except Exception:
            context.log.warn("Failed to upload cost information to S3.", exc_info=True)
            # if we're not submitting via GQL API as well, raise the exception more loudly
            if not submit_gql:
                raise
    except ImportError:
        context.log.warn(
            "Dagster insights dependencies not installed. In the future, you will need to install dagster-cloud[insights] to use this feature."
        )
        # if we're not submitting via GQL API as well, raise the exception more loudly
        if not submit_gql:
            raise

    # early exit if we're not submitting via GQL API as well
    if not submit_gql:
        return

    cost_info_input = [
        {
            "opaqueId": opaque_id,
            "cost": float(cost),
        }
        for opaque_id, cost, _ in cost_information
    ]

    # Chunk the cost information & keep each opaque id in the same chunk
    # to avoid the cost information for a single asset being split across
    # multiple steps
    for chunk in chunk_by_opaque_id(
        [(opaque_id, cost) for opaque_id, cost, _ in cost_information], chunk_size
    ):
        cost_info_input = [
            {
                "opaqueId": opaque_id,
                "cost": float(cost),
            }
            for opaque_id, cost in chunk
        ]

        result = query_graphql_from_instance(
            context.instance,
            PUT_COST_INFORMATION_MUTATION,
            variables={
                "costInfo": cost_info_input,
                "metricName": metric_name,
                "start": start,
                "end": end,
            },
        )
        if (
            result["submitCostInformationForMetrics"]["__typename"]
            != "CreateOrUpdateMetricsSuccess"
        ):
            raise RuntimeError("Failed to submit cost information", result)


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
