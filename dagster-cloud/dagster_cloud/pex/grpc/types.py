from typing import List, NamedTuple

import dagster._check as check
from dagster._serdes import create_snapshot_id, whitelist_for_serdes
from dagster_cloud_cli.core.workspace import CodeDeploymentMetadata


# Enough to uniquely create (and later identify) a given PEX server - the
# timestamp is what allows us to reload a location without changing its PEX and
# still create a new server process.
@whitelist_for_serdes
class PexServerHandle(
    NamedTuple(
        "_PexServerHandle",
        [
            ("deployment_name", str),
            ("location_name", str),
            ("metadata_update_timestamp", int),
        ],
    )
):
    def __new__(cls, deployment_name: str, location_name: str, metadata_update_timestamp: int):
        return super(PexServerHandle, cls).__new__(
            cls,
            check.str_param(deployment_name, "deployment_name"),
            check.str_param(location_name, "location_name"),
            check.int_param(metadata_update_timestamp, "metadata_update_timestamp"),
        )

    def get_id(self) -> str:
        return create_snapshot_id(self)


@whitelist_for_serdes
class CreatePexServerArgs(
    NamedTuple(
        "_CreatePexServerArgs",
        [
            ("server_handle", PexServerHandle),
            ("code_deployment_metadata", CodeDeploymentMetadata),
        ],
    )
):
    pass


@whitelist_for_serdes
class CreatePexServerResponse(
    NamedTuple(
        "_CreatePexServerResponse",
        [],
    )
):
    pass


@whitelist_for_serdes
class GetPexServersArgs(
    NamedTuple(
        "_GetPexServersArgs",
        [
            ("deployment_name", str),
            ("location_name", str),
        ],
    )
):
    pass


@whitelist_for_serdes
class GetPexServersResponse(
    NamedTuple(
        "_GetPexServersResponse",
        [
            ("server_handles", List[PexServerHandle]),
        ],
    )
):
    pass


@whitelist_for_serdes
class ShutdownPexServerArgs(
    NamedTuple(
        "_ShutdownPexServerArgs",
        [
            ("server_handle", PexServerHandle),
        ],
    )
):
    pass


@whitelist_for_serdes
class ShutdownPexServerResponse(
    NamedTuple(
        "_ShutdownPexServerResponse",
        [],
    )
):
    pass
