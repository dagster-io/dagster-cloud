from abc import abstractproperty
from enum import Enum
from typing import Dict

from dagster._core.definitions.metadata import FloatMetadataValue, MetadataValue
from pydantic import BaseModel


class AnomalyDetectionModelVersion(Enum):
    FRESHNESS_BETA = "FRESHNESS_BETA"


### INTERNAL MODEL PARAMETER SETS ###


class InternalModelParams(BaseModel):
    @abstractproperty
    def model_version(self) -> AnomalyDetectionModelVersion:
        raise NotImplementedError("Subclasses must implement this method")


class InternalBetaFreshnessAnomalyDetectionParams(InternalModelParams):
    sensitivity: float
    asset_key_user_string: str

    @property
    def model_version(self) -> AnomalyDetectionModelVersion:
        return AnomalyDetectionModelVersion.FRESHNESS_BETA


### USER FACING MODEL PARAMETER SETS ###


class AnomalyDetectionModelParams(BaseModel):
    @abstractproperty
    def model_version(self) -> AnomalyDetectionModelVersion:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractproperty
    def as_metadata(self) -> Dict[str, MetadataValue]:
        raise NotImplementedError("Subclasses must implement this method")


class BetaFreshnessAnomalyDetectionParams(AnomalyDetectionModelParams):
    sensitivity: float

    @property
    def model_version(self) -> AnomalyDetectionModelVersion:
        return AnomalyDetectionModelVersion.FRESHNESS_BETA

    @property
    def as_metadata(self) -> Dict[str, MetadataValue]:
        return {
            "sensitivity": FloatMetadataValue(self.sensitivity),
        }
