from collections import namedtuple

from dagster import check
from dagster.serdes.utils import create_snapshot_id


class SerializableNamedtupleMapDiff(
    namedtuple(
        "_SerializableNamedtupleMapDiff",
        "to_add to_update to_remove",
    )
):
    def __new__(
        cls,
        to_add,
        to_update,
        to_remove,
    ):
        return super(SerializableNamedtupleMapDiff, cls).__new__(
            cls,
            check.set_param(to_add, "to_add", str),
            check.set_param(to_update, "to_update", str),
            check.set_param(to_remove, "to_remove", str),
        )


def diff_serializable_namedtuple_map(desired_map, actual_map, force_update_keys=None):
    desired_keys = set(desired_map.keys())
    actual_keys = set(actual_map.keys())
    force_update_keys = check.opt_set_param(force_update_keys, "force_update_keys", str)

    to_add = desired_keys.difference(actual_keys)
    to_remove = actual_keys.difference(desired_keys)

    existing = actual_keys.intersection(desired_keys)

    to_update = {
        existing_key
        for existing_key in existing
        if create_snapshot_id(desired_map[existing_key])
        != create_snapshot_id(actual_map[existing_key])
        or existing_key in force_update_keys
    }

    return SerializableNamedtupleMapDiff(to_add, to_update, to_remove)
