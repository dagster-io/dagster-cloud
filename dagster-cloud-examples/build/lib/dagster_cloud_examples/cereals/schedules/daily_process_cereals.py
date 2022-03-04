from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs.compute_cereal_properties.process_cereals import compute_cereal_properties

daily_compute_cereal_properties = ScheduleDefinition(
    name="daily_compute_cereal_properties",
    cron_schedule="0 0 * * *",
    job=compute_cereal_properties,
    default_status=DefaultScheduleStatus.RUNNING,
)
