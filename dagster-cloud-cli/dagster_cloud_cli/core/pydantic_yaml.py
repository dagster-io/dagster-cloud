"""Use a pydantic definition to validate dagster_cloud.yaml."""

from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Extra, Field, root_validator, validator


class CodeSource(BaseModel, extra=Extra.forbid):
    package_name: Optional[str]
    module_name: Optional[str]
    python_file: Optional[str]

    @root_validator
    def exactly_one_source_defined(
        cls, values: Dict[str, Optional[str]]
    ) -> Dict[str, Optional[str]]:
        defined = [key for key, value in values.items() if value]
        if len(defined) > 1:
            raise ValueError(
                "only one of the following fields should be defined: " + ", ".join(defined)
            )
        elif not defined:
            raise ValueError("one of package_name, module_name and python_file must be specified")
        return values


class Build(BaseModel, extra=Extra.forbid):
    directory: Optional[str]
    registry: Optional[str]


class Location(BaseModel, extra=Extra.forbid):
    location_name: str
    code_source: Optional[CodeSource]
    build: Optional[Build]
    working_directory: Optional[str]
    image: Optional[str]
    executable_path: Optional[str]
    attribute: Optional[str]
    container_context: Optional[Dict[str, Any]]


class DagsterCloudYaml(BaseModel, extra=Extra.forbid):
    locations: List[Location] = Field(description="List of code locations")

    @validator("locations")
    def no_duplicate_names(cls, values: List[Location]) -> List[Location]:
        names = set()
        for location in values:
            if location.location_name in names:
                raise ValueError(f"duplicate location name: {location.location_name}")
            names.add(location.location_name)
        return values


def load_dagster_cloud_yaml(text) -> DagsterCloudYaml:
    parsed = DagsterCloudYaml.parse_obj(yaml.load(text, Loader=yaml.SafeLoader))
    return parsed
