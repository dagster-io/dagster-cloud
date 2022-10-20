"Decodes PexMetadata to download S3 files locally and setup a runnable PEX environment."
import os
from typing import Dict, List, NamedTuple, Optional
from uuid import uuid4

from dagster import _check as check
from dagster_cloud_cli.core.workspace import PexMetadata

DEFAULT_PEX_FILES_DIR = "/tmp/pex-files"


def _download_from_s3(filename: str, local_filepath: str):
    # Lazy import boto3 to avoid a hard dependency during module load
    import boto3

    s3 = boto3.client("s3")  # type: ignore[attr-defined]

    # TODO: move the bucket and prefix to pex_metdata
    s3_bucket_name = os.environ["DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_BUCKET"]
    # prefix is typically org-storage/{org_public_id}
    s3_prefix = os.environ["DAGSTER_CLOUD_SERVERLESS_STORAGE_S3_PREFIX"]

    s3_key = f"{s3_prefix}/pex/{filename}"
    # write to different file and rename for read safety
    local_tmp_filepath = local_filepath + "-" + str(uuid4())
    s3.download_file(
        Bucket=s3_bucket_name,
        Key=s3_key,
        Filename=local_tmp_filepath,
    )
    os.rename(local_tmp_filepath, local_filepath)


class PexExecutable(
    NamedTuple(
        "_PexExecutable",
        [
            ("source_path", str),
            ("all_paths", List[str]),
            ("environ", Dict[str, str]),
        ],
    )
):
    def __new__(
        cls,
        source_path: str,
        all_paths: List[str],
        environ: Dict[str, str],
    ):
        return super(PexExecutable, cls).__new__(
            cls,
            check.str_param(source_path, "source_path"),
            check.list_param(all_paths, "all_paths", str),
            check.dict_param(environ, "environ", str, str),
        )


class PexS3Registry:
    def __init__(self, local_pex_files_dir: Optional[str] = None):
        self._local_pex_files_dir = (
            local_pex_files_dir if local_pex_files_dir else DEFAULT_PEX_FILES_DIR
        )
        os.makedirs(self._local_pex_files_dir, exist_ok=True)

    def get_pex_executable(self, pex_metadata: PexMetadata) -> PexExecutable:
        if "=" not in pex_metadata.pex_tag:
            raise ValueError(f"Invalid pex tag, no prefix in {pex_metadata.pex_tag!r}")
        prefix, filenames = pex_metadata.pex_tag.split("=", 1)
        if prefix != "files":
            raise ValueError(
                f'Expected pex_tag prefix "files=" not found in tag {pex_metadata.pex_tag!r}'
            )

        pex_filenames = filenames.split(":")
        deps_pex_filepaths = []
        source_pex_filepath = None

        for filename in pex_filenames:
            local_filepath = os.path.join(self._local_pex_files_dir, filename)

            # no need to download if we already have this file - these
            # files have a content hash suffix so name equality implies content is same
            if not os.path.exists(local_filepath):
                if os.getenv("S3_PEX_DISABLED"):
                    raise ValueError(
                        f"File {local_filepath} not found for pex tag {pex_metadata.pex_tag}, S3_PEX_DISABLED"
                    )
                _download_from_s3(filename, local_filepath)

            if filename.startswith("source-"):
                source_pex_filepath = local_filepath
                # make it executable
                os.chmod(source_pex_filepath, 0o775)
            else:
                deps_pex_filepaths.append(local_filepath)

        if not source_pex_filepath:
            raise ValueError("Invalid pex_tag has no source pex: %r" % pex_metadata.pex_tag)

        return PexExecutable(
            source_pex_filepath,
            [source_pex_filepath] + deps_pex_filepaths,
            {"PEX_PATH": ":".join(deps_pex_filepaths)},
        )
