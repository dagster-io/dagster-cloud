from dagster import repository

from .jobs.compute_cereal_properties.process_cereals import compute_cereal_properties
from .schedules.daily_process_cereals import daily_compute_cereal_properties


@repository
def cereal_repository():
    return [compute_cereal_properties, daily_compute_cereal_properties]
