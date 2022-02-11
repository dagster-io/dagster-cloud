from dagster import repository

from .process_cereals import compute_cereal_properties


@repository
def cereal_repository():
    return [compute_cereal_properties]
