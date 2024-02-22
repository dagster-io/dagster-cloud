from contextlib import contextmanager, nullcontext
from typing import (
    Iterator,
)

from dagster import AssetKey, AssetObservation, JobDefinition
from dagster._annotations import experimental
from dagster_gcp import BigQueryResource
from dagster_gcp.bigquery.utils import setup_gcp_creds
from google.cloud import bigquery

from dagster_cloud.dagster_insights.insights_utils import (
    get_current_context_and_asset_key,
)

OUTPUT_NON_ASSET_SIGIL = "__bigquery_query_metadata_"


class WrappedBigQueryClient(bigquery.Client):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._query_bytes = []

    def query(self, *args, **kwargs) -> bigquery.QueryJob:
        bq_job = super().query(*args, **kwargs)
        self._query_bytes.append(bq_job.total_bytes_billed)
        return bq_job

    @property
    def has_bytes_billed(self) -> bool:
        return len(self._query_bytes) > 0

    @property
    def total_bytes_billed(self) -> int:
        return sum(self._query_bytes)


@experimental
class InsightsBigQueryResource(BigQueryResource):
    """A wrapper around :py:class:`BigQueryResource` which automatically collects metadata about
    BigQuery costs which can be attributed to Dagster jobs and assets.

    A simple example of loading data into BigQuery and subsequently querying that data is shown below:

    Examples:
        .. code-block:: python

            from dagster import job, op
            from dagster_gcp import BigQueryResource
            from dagster_insights import InsightsBigQueryResource

            @op
            def get_one(bigquery_resource: BigQueryResource):
                with bigquery_resource.get_client() as client:
                    client.query("SELECT * FROM my_dataset.my_table")

            @job
            def my_bigquery_job():
                get_one()

            my_bigquery_job.execute_in_process(
                resources={ "bigquery": InsightsBigQueryResource(project="my-project") }
            )
    """

    @contextmanager
    def get_client(self) -> Iterator[bigquery.Client]:
        context, asset_key = get_current_context_and_asset_key()

        with setup_gcp_creds(self.gcp_credentials) if self.gcp_credentials else nullcontext():
            client = WrappedBigQueryClient(project=self.project, location=self.location)

            yield client

            if client.has_bytes_billed:
                if not asset_key:
                    asset_key = _bigquery_asset_key_for_job(context.job_def)

                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata={
                            "bytes_billed": client.total_bytes_billed,
                        },
                    )
                )


def _bigquery_asset_key_for_job(job: JobDefinition) -> AssetKey:
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])
