from dagster import job

from .ops import (
    display_results,
    download_cereals,
    find_highest_calorie_cereal,
    find_highest_protein_cereal,
)


@job
def compute_cereal_properties():
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )
