import os
from typing import Optional, Sequence, Union, cast

from dagster import (
    _check as check,
    asset_check,
)
from dagster._core.definitions.asset_check_result import AssetCheckResult
from dagster._core.definitions.asset_check_spec import AssetCheckSeverity
from dagster._core.definitions.asset_checks import AssetChecksDefinition
from dagster._core.definitions.assets import AssetsDefinition
from dagster._core.definitions.events import CoercibleToAssetKey
from dagster._core.definitions.source_asset import SourceAsset
from dagster._core.errors import (
    DagsterError,
    DagsterInvariantViolationError,
)
from dagster._core.execution.context.compute import AssetExecutionContext
from dagster._core.instance import DagsterInstance
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from dagster_cloud import DagsterCloudAgentInstance

from .mutation import ANOMALY_DETECTION_INFERENCE_MUTATION
from .types import (
    AnomalyDetectionModelParams,
    BetaFreshnessAnomalyDetectionParams,
)

DEFAULT_MODEL_PARAMS = BetaFreshnessAnomalyDetectionParams(sensitivity=0.1)


class DagsterCloudAnomalyDetectionFailed(DagsterError):
    """Raised when an anomaly detection check fails host-side."""


def _build_check_for_asset(
    asset: Union[CoercibleToAssetKey, AssetsDefinition, SourceAsset],
    params: AnomalyDetectionModelParams,
) -> AssetChecksDefinition:
    @asset_check(
        asset=asset,
        description=f"Detects anomalies in the freshness of the asset using model {params.model_version.value.lower()}.",
        name="freshness_anomaly_detection",
    )
    def the_check(context: AssetExecutionContext) -> AssetCheckResult:
        if not _is_agent_instance(context.instance):
            raise DagsterInvariantViolationError(
                f"This anomaly detection check is not being launched from a dagster agent. "
                "Anomaly detection is only available for dagster cloud deployments."
                f"Instance type: {type(context.instance)}."
            )
        instance = cast(DagsterCloudAgentInstance, context.instance)
        transport = RequestsHTTPTransport(
            url=os.getenv("DAGSTER_METRICS_DAGIT_URL", f"{instance.dagit_url}graphql"),
            use_json=True,
            timeout=300,
            headers={"Dagster-Cloud-Api-Token": instance.dagster_cloud_agent_token},
        )
        client = Client(transport=transport, fetch_schema_from_transport=True)
        asset_key = next(iter(context.assets_def.check_keys)).asset_key
        if not context.job_def.asset_layer.has(asset_key):
            raise Exception(f"Could not find targeted asset {asset_key.to_string()}.")
        result = client.execute(
            gql(ANOMALY_DETECTION_INFERENCE_MUTATION),
            {
                "modelVersion": params.model_version.value,
                "params": {
                    **dict(params),
                    "asset_key_user_string": asset_key.to_user_string(),
                },
            },
        )
        if result["anomalyDetectionInference"]["__typename"] != "AnomalyDetectionSuccess":
            raise DagsterCloudAnomalyDetectionFailed(
                f"Anomaly detection failed: {result['anomalyDetectionInference']['message']}"
            )
        response = result["anomalyDetectionInference"]["response"]
        overdue_seconds = check.float_param(response["overdue_seconds"], "overdue_seconds")
        expected_event_timestamp = response["overdue_deadline_timestamp"]
        model_training_range_start = response["model_training_range_start_timestamp"]
        model_training_range_end = response["model_training_range_end_timestamp"]
        metadata = {
            "model_params": {**params.as_metadata},
            "model_version": params.model_version.value,
            "model_training_range_start_timestamp": model_training_range_start,
            "model_training_range_end_timestamp": model_training_range_end,
            "overdue_deadline_timestamp": expected_event_timestamp,
        }
        if overdue_seconds > 0:
            metadata["overdue_minutes"] = overdue_seconds / 60
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                metadata=metadata,
            )
        else:
            return AssetCheckResult(passed=True, metadata=metadata)

    return the_check


def build_anomaly_detection_freshness_checks(
    *,
    assets: Sequence[Union[CoercibleToAssetKey, AssetsDefinition, SourceAsset]],
    params: Optional[AnomalyDetectionModelParams],
) -> Sequence[AssetChecksDefinition]:
    """Builds a list of asset checks which utilize anomaly detection algorithms to
    determine the freshness of data.

    Args:
        assets (Sequence[Union[CoercibleToAssetKey, AssetsDefinition, SourceAsset]]): The assets to construct checks for. For each passed in
            asset, there will be a corresponding constructed `AssetChecksDefinition`.
        params (AnomalyDetectionModelParams): The parameters to use for the model. The parameterization corresponds to the model used.

    Returns:
        Sequence[AssetChecksDefinition]: A list of `AssetChecksDefinition` objects, each corresponding to an asset in the `assets` parameter.

    Examples:
        .. code-block:: python

            from dagster_cloud import build_anomaly_detection_freshness_checks, AnomalyDetectionModel, BetaFreshnessAnomalyDetectionParams

            checks = build_anomaly_detection_freshness_checks(
                assets=[AssetKey("foo_asset"), AssetKey("foo_asset")],
                params=BetaFreshnessAnomalyDetectionParams(sensitivity=0.1),
            )
    """
    params = check.opt_inst_param(
        params, "params", AnomalyDetectionModelParams, DEFAULT_MODEL_PARAMS
    )
    return [_build_check_for_asset(asset, params) for asset in assets]


def _is_agent_instance(instance: DagsterInstance) -> bool:
    if hasattr(instance, "dagster_cloud_agent_token") and hasattr(instance, "dagit_url"):
        return True
    return False
