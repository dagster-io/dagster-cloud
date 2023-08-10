import datetime
import io
import os
import pickle
from typing import Sequence, Tuple, Union

import boto3
import dagster._check as check
import requests
from dagster import InputContext, MemoizableIOManager, OutputContext, io_manager
from dagster._utils import PICKLE_PROTOCOL
from dateutil import parser

ECS_AGENT_IP = "169.254.170.2"


class PickledObjectServerlessIOManager(MemoizableIOManager):
    def __init__(
        self,
        s3_bucket,
        s3_prefix,
    ):
        self._bucket = check.str_param(s3_bucket, "s3_bucket")
        self._s3_prefix = check.str_param(s3_prefix, "s3_prefix")
        self._boto_session, self._boto_session_expiration = self._refresh_boto_session()

    def _refresh_boto_session(self) -> Tuple[boto3.Session, datetime.datetime]:
        # We have to do this whacky way to get credentials to ensure that we get iam role
        # we assigned to the task. If we used the default boto behavior, it could get overriden
        # when users set AWS env vars.
        relative_uri = os.environ["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"]
        aws_creds = requests.get(f"http://{ECS_AGENT_IP}{relative_uri}").json()
        session = boto3.Session(
            aws_access_key_id=aws_creds["AccessKeyId"],
            aws_secret_access_key=aws_creds["SecretAccessKey"],
            aws_session_token=aws_creds["Token"],
        )
        expiration = parser.parse(aws_creds["Expiration"])
        return session, expiration

    @property
    def _s3(self):
        if self._boto_session_expiration <= datetime.datetime.now(
            self._boto_session_expiration.tzinfo
        ) + datetime.timedelta(minutes=5):
            self._boto_session, self._boto_session_expiration = self._refresh_boto_session()
        return self._boto_session.client("s3", region_name="us-west-2")

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        path: Sequence[str]
        if context.has_asset_key:
            path = context.get_asset_identifier()
        else:
            path = ["storage", *context.get_identifier()]

        return "/".join([self._s3_prefix, *path])

    def has_output(self, context):
        key = self._get_path(context)
        return self._has_object(key)

    def _rm_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        # delete_object wont fail even if the item has been deleted.
        self._s3.delete_object(Bucket=self._bucket, Key=key)

    def _has_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        found_object = False

        try:
            self._s3.get_object(Bucket=self._bucket, Key=key)
            found_object = True
        except self._s3.exceptions.NoSuchKey:
            found_object = False

        return found_object

    def load_input(self, context):
        if context.dagster_type.typing_type == type(None):
            return None

        key = self._get_path(context)
        check.invariant(self._has_object(key), "Input not found. It may have expired.")
        obj = pickle.loads(self._s3.get_object(Bucket=self._bucket, Key=key)["Body"].read())

        return obj

    def handle_output(self, context, obj):
        if context.dagster_type.typing_type == type(None):
            check.invariant(
                obj is None,
                "Output had Nothing type or 'None' annotation, but handle_output received"
                f" value that was not None and was of type {type(obj)}.",
            )
            return None

        key = self._get_path(context)

        if self._has_object(key):
            self._rm_object(key)

        pickled_obj = pickle.dumps(obj, PICKLE_PROTOCOL)
        pickled_obj_bytes = io.BytesIO(pickled_obj)
        self._s3.upload_fileobj(
            pickled_obj_bytes,
            self._bucket,
            key,
        )


@io_manager
def serverless_io_manager(init_context):
    bucket = os.getenv("DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_BUCKET")
    prefix = os.getenv("DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_PREFIX")
    check.invariant(
        bucket and prefix,
        "The serverless_io_manager is only supported when running on Dagster Cloud Serverless."
        " DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_BUCKET or"
        " DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_PREFIX not found.",
    )

    deployment_name = init_context.instance.deployment_name

    return PickledObjectServerlessIOManager(
        bucket, s3_prefix=f"{prefix}/io_storage/{deployment_name}"
    )
