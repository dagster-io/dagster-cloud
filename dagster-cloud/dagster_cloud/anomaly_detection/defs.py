from typing import Iterable, Optional, Sequence, Union, cast

from dagster import (
    AssetCheckExecutionContext,
    MetadataValue,
    _check as check,
)
from dagster._core.definitions.asset_check_result import AssetCheckResult
from dagster._core.definitions.asset_check_spec import AssetCheckSeverity, AssetCheckSpec
from dagster._core.definitions.asset_checks import AssetChecksDefinition
from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.assets import AssetsDefinition
from dagster._core.definitions.decorators.asset_check_decorator import multi_asset_check
from dagster._core.definitions.events import CoercibleToAssetKey
from dagster._core.definitions.freshness_checks.utils import (
    asset_to_keys_iterable,
    seconds_in_words,
    unique_id_from_asset_keys,
)
from dagster._core.definitions.source_asset import SourceAsset
from dagster._core.errors import (
    DagsterError,
    DagsterInvariantViolationError,
)
from dagster._core.instance import DagsterInstance
from dagster_cloud_cli.core.graphql_client import create_cloud_webserver_client

from dagster_cloud import DagsterCloudAgentInstance

from .mutation import ANOMALY_DETECTION_INFERENCE_MUTATION
from .types import (
    AnomalyDetectionModelParams,
    BetaFreshnessAnomalyDetectionParams,
)

DEFAULT_MODEL_PARAMS = BetaFreshnessAnomalyDetectionParams(sensitivity=0.1)


class DagsterCloudAnomalyDetectionFailed(DagsterError):
    """Raised when an anomaly detection check fails host-side."""


def _build_check_for_assets(
    asset_keys: Sequence[AssetKey],
    params: AnomalyDetectionModelParams,
) -> AssetChecksDefinition:
    @multi_asset_check(
        specs=[
            AssetCheckSpec(
                name="anomaly_detection_freshness_check",
                description=f"Detects anomalies in the freshness of the asset using model {params.model_version.value.lower()}.",
                asset=asset_key,
            )
            for asset_key in asset_keys
        ],
        can_subset=True,
        name=f"anomaly_detection_freshness_check_{unique_id_from_asset_keys(asset_keys)}",
    )
    def the_check(context: AssetCheckExecutionContext) -> Iterable[AssetCheckResult]:
        if not _is_agent_instance(context.instance):
            raise DagsterInvariantViolationError(
                f"This anomaly detection check is not being launched from a dagster agent. "
                "Anomaly detection is only available for dagster cloud deployments."
                f"Instance type: {type(context.instance)}."
            )
        instance = cast(DagsterCloudAgentInstance, context.instance)
        with create_cloud_webserver_client(
            instance.dagit_url[:-1]
            if instance.dagit_url.endswith("/")
            else instance.dagit_url,  # Remove trailing slash
            check.str_param(instance.dagster_cloud_agent_token, "dagster_cloud_agent_token"),
        ) as client:
            for check_key in context.selected_asset_check_keys:
                asset_key = check_key.asset_key
                if not context.job_def.asset_layer.has(asset_key):
                    raise Exception(f"Could not find targeted asset {asset_key.to_string()}.")
                result = client.execute(
                    ANOMALY_DETECTION_INFERENCE_MUTATION,
                    {
                        "modelVersion": params.model_version.value,
                        "params": {
                            **dict(params),
                            "asset_key_user_string": asset_key.to_user_string(),
                        },
                    },
                )
                data = result["data"]["anomalyDetectionInference"]
                metadata = {
                    "model_params": {**params.as_metadata},
                    "model_version": params.model_version.value,
                }
                if data["__typename"] != "AnomalyDetectionSuccess":
                    yield handle_anomaly_detection_inference_failure(
                        data, metadata, params, asset_key
                    )
                    continue
                response = result["data"]["anomalyDetectionInference"]["response"]
                overdue_seconds = check.float_param(response["overdue_seconds"], "overdue_seconds")
                overdue_deadline_timestamp = response["overdue_deadline_timestamp"]
                metadata["overdue_deadline_timestamp"] = MetadataValue.timestamp(
                    overdue_deadline_timestamp
                )
                metadata["model_training_range_start_timestamp"] = MetadataValue.timestamp(
                    response["model_training_range_start_timestamp"]
                )
                metadata["model_training_range_end_timestamp"] = MetadataValue.timestamp(
                    response["model_training_range_end_timestamp"]
                )

                last_updated_timestamp = response["last_updated_timestamp"]
                if last_updated_timestamp is None:
                    yield AssetCheckResult(
                        passed=True,
                        description="The asset has never been materialized or otherwise observed to have been updated",
                    )
                    continue

                evaluation_timestamp = response["evaluation_timestamp"]
                last_update_lag_str = seconds_in_words(
                    evaluation_timestamp - last_updated_timestamp
                )
                expected_lag_str = seconds_in_words(
                    overdue_deadline_timestamp - last_updated_timestamp
                )
                gt_or_lte_str = "greater than" if overdue_seconds > 0 else "less than or equal to"
                lag_comparison_str = (
                    f"At the time of this check's evaluation, {last_update_lag_str} had passed since its "
                    f"last update. This is {gt_or_lte_str} the allowed {expected_lag_str} threshold, which "
                    "is based on its prior history of updates."
                )

                if overdue_seconds > 0:
                    metadata["overdue_minutes"] = round(overdue_seconds / 60, 2)

                    yield AssetCheckResult(
                        passed=False,
                        severity=AssetCheckSeverity.WARN,
                        metadata=metadata,
                        description=f"The asset is overdue for an update. {lag_comparison_str}",
                        asset_key=asset_key,
                    )
                else:
                    yield AssetCheckResult(
                        passed=True,
                        metadata=metadata,
                        description=f"The asset is fresh. {lag_comparison_str}",
                        asset_key=asset_key,
                    )

    return the_check


def handle_anomaly_detection_inference_failure(
    data: dict, metadata: dict, params: AnomalyDetectionModelParams, asset_key: AssetKey
) -> AssetCheckResult:
    if (
        data["__typename"] == "AnomalyDetectionFailure"
        and data["message"] == params.model_version.minimum_required_records_msg
    ):
        # Intercept failure in the case of not enough records, and return a pass to avoid
        # being too noisy with failures.
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata=metadata,
            description=data["message"],
            asset_key=asset_key,
        )
    raise DagsterCloudAnomalyDetectionFailed(f"Anomaly detection failed: {data['message']}")


def build_anomaly_detection_freshness_checks(
    *,
    assets: Sequence[Union[CoercibleToAssetKey, AssetsDefinition, SourceAsset]],
    params: Optional[AnomalyDetectionModelParams],
) -> AssetChecksDefinition:
    """Builds a list of asset checks which utilize anomaly detection algorithms to
    determine the freshness of data.

    Args:
        assets (Sequence[Union[CoercibleToAssetKey, AssetsDefinition, SourceAsset]]): The assets to construct checks for. For each passed in
            asset, there will be a corresponding constructed `AssetChecksDefinition`.
        params (AnomalyDetectionModelParams): The parameters to use for the model. The parameterization corresponds to the model used.

    Returns:
        AssetChecksDefinition: A list of `AssetChecksDefinition` objects, each corresponding to an asset in the `assets` parameter.

    Examples:
        .. code-block:: python

            from dagster_cloud import build_anomaly_detection_freshness_checks, BetaFreshnessAnomalyDetectionParams

            checks_def = build_anomaly_detection_freshness_checks(
                assets=[AssetKey("foo_asset"), AssetKey("foo_asset")],
                params=BetaFreshnessAnomalyDetectionParams(sensitivity=0.1),
            )
    """
    params = check.opt_inst_param(
        params, "params", AnomalyDetectionModelParams, DEFAULT_MODEL_PARAMS
    )
    return _build_check_for_assets(
        [asset_key for asset in assets for asset_key in asset_to_keys_iterable(asset)], params
    )


def _is_agent_instance(instance: DagsterInstance) -> bool:
    if hasattr(instance, "dagster_cloud_agent_token") and hasattr(instance, "dagit_url"):
        return True
    return False
