"""Use a pydantic definition to validate dagster_cloud.yaml."""

from typing import Any, Optional

import yaml
from pydantic import BaseModel, Extra, Field, model_validator, validator


class CodeSource(BaseModel, extra=Extra.forbid):
    package_name: Optional[str] = None
    module_name: Optional[str] = None
    python_file: Optional[str] = None

    @model_validator(mode="before")
    def exactly_one_source_defined(
        cls, values: dict[str, Optional[str]]
    ) -> dict[str, Optional[str]]:
        defined = [key for key, value in values.items() if value]
        if len(defined) > 1:
            raise ValueError(
                "only one of the following fields should be defined: " + ", ".join(defined)
            )
        elif not defined:
            raise ValueError("one of package_name, module_name and python_file must be specified")
        return values


class Build(BaseModel, extra=Extra.forbid):
    directory: Optional[str] = None
    registry: Optional[str] = None


class Location(BaseModel, extra=Extra.forbid):
    location_name: str
    code_source: Optional[CodeSource] = None
    build: Optional[Build] = None
    working_directory: Optional[str] = None
    image: Optional[str] = None
    executable_path: Optional[str] = None
    attribute: Optional[str] = None
    container_context: Optional[dict[str, Any]] = None
    agent_queue: Optional[str] = None


class DagsterCloudYaml(BaseModel, extra=Extra.forbid):
    locations: list[Location] = Field(description="List of code locations")

    @validator("locations")
    def no_duplicate_names(cls, v: list[Location]) -> list[Location]:
        names = set()
        for location in v:
            if location.location_name in names:
                raise ValueError(f"duplicate location name: {location.location_name}")
            names.add(location.location_name)
        return v


def load_dagster_cloud_yaml(text) -> DagsterCloudYaml:
    parsed = DagsterCloudYaml.parse_obj(yaml.load(text, Loader=yaml.SafeLoader))
    return parsed
