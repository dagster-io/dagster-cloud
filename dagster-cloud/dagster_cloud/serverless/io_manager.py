import io
import os
import pickle
from typing import Sequence, Union

import boto3
import dagster._check as check
import requests
from dagster import InputContext, MemoizableIOManager, OutputContext, io_manager
from dagster._utils import PICKLE_PROTOCOL


class PickledObjectServerlessIOManager(MemoizableIOManager):
    def __init__(
        self,
        s3_bucket,
        s3_session,
        s3_prefix=None,
    ):
        self.bucket = check.str_param(s3_bucket, "s3_bucket")
        self.s3_prefix = check.opt_str_param(s3_prefix, "s3_prefix")
        self.s3 = s3_session

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        path: Sequence[str]
        if context.has_asset_key:
            path = context.get_asset_identifier()
        else:
            path = ["storage", *context.get_identifier()]

        return "/".join([self.s3_prefix, *path])

    def has_output(self, context):
        key = self._get_path(context)
        return self._has_object(key)

    def _rm_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        # delete_object wont fail even if the item has been deleted.
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def _has_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        found_object = False

        try:
            self.s3.get_object(Bucket=self.bucket, Key=key)
            found_object = True
        except self.s3.exceptions.NoSuchKey:
            found_object = False

        return found_object

    def load_input(self, context):
        if context.dagster_type.typing_type == type(None):
            return None

        key = self._get_path(context)
        obj = pickle.loads(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())

        return obj

    def handle_output(self, context, obj):
        if context.dagster_type.typing_type == type(None):
            check.invariant(
                obj is None,
                (
                    "Output had Nothing type or 'None' annotation, but handle_output received"
                    f" value that was not None and was of type {type(obj)}."
                ),
            )
            return None

        key = self._get_path(context)

        if self._has_object(key):
            self._rm_object(key)

        pickled_obj = pickle.dumps(obj, PICKLE_PROTOCOL)
        pickled_obj_bytes = io.BytesIO(pickled_obj)
        self.s3.upload_fileobj(pickled_obj_bytes, self.bucket, key)


ECS_AGENT_IP = "169.254.170.2"


def _get_s3_client():
    relative_uri = os.environ["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"]
    aws_creds = requests.get(f"http://{ECS_AGENT_IP}{relative_uri}").json()
    session = boto3.Session(
        aws_access_key_id=aws_creds["AccessKeyId"],
        aws_secret_access_key=aws_creds["SecretAccessKey"],
        aws_session_token=aws_creds["Token"],
    )
    return session.client("s3")


@io_manager
def serverless_io_manager(init_context):
    bucket = os.getenv("DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_BUCKET")
    prefix = os.getenv("DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_PREFIX")
    check.invariant(
        bucket and prefix,
        (
            "The serverless_io_manager is only supported when running on Dagster Cloud Serverless."
            " DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_BUCKET or"
            " DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_PREFIX not found."
        ),
    )

    deployment_name = init_context.instance.deployment_name

    return PickledObjectServerlessIOManager(
        bucket, _get_s3_client(), s3_prefix=f"{prefix}/io_storage/{deployment_name}"
    )
