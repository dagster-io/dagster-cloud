"""Decodes PexMetadata to download S3 files locally and setup a runnable PEX environment."""
import hashlib
import logging
import os
import shutil
import subprocess
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
            ("working_directory", Optional[str]),
            ("venv_directory", Optional[str]),
        ],
    )
):
    def __new__(
        cls,
        source_path: str,
        all_paths: List[str],
        environ: Dict[str, str],
        working_directory: Optional[str],
        venv_directory: Optional[str],
    ):
        return super(PexExecutable, cls).__new__(
            cls,
            check.str_param(source_path, "source_path"),
            check.list_param(all_paths, "all_paths", str),
            check.dict_param(environ, "environ", str, str),
            check.opt_str_param(working_directory, "working_directory"),
            check.opt_str_param(venv_directory, "venv_directory"),
        )


class PexS3Registry:
    def __init__(self, local_pex_files_dir: Optional[str] = None):
        self._local_pex_files_dir = (
            local_pex_files_dir if local_pex_files_dir else DEFAULT_PEX_FILES_DIR
        )
        os.makedirs(self._local_pex_files_dir, exist_ok=True)
        self.working_dirs: Dict[
            str, str
        ] = {}  # once unpacked, working dirs dont change so we cache them

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
                        f"File {local_filepath} not found for pex tag {pex_metadata.pex_tag},"
                        " S3_PEX_DISABLED"
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

        # we unpack the pex files into a venv
        pex_venv_dir = self.venv_dir_for(pex_metadata.pex_tag)

        if not os.path.exists(pex_venv_dir):
            venv_installed = self.install_venv(
                pex_venv_dir, source_pex_filepath, deps_pex_filepaths
            )
        else:
            venv_installed = True

        if venv_installed:
            # unpacking a venv from a pex provides this pex script as the entrypoint
            entrypoint = os.path.join(pex_venv_dir, "pex")

            # inject the venv's bin directory into the path
            bin_path = os.path.join(pex_venv_dir, "bin")
            env = {"PATH": bin_path + ":" + os.environ["PATH"]}
        else:
            # TODO: remove fallback, once we're sure pex venv is reliable
            logging.info("Ignoring failure to install venv")
            entrypoint = source_pex_filepath
            env = {"PEX_PATH": ":".join(deps_pex_filepaths)}

        working_dir = self.get_working_dir_for_pex(entrypoint)

        return PexExecutable(
            entrypoint,
            [source_pex_filepath] + deps_pex_filepaths,
            env,
            working_dir,
            venv_directory=pex_venv_dir if venv_installed else None,
        )

    def venv_dir_for(self, pex_tag: str):
        # Use a short name for better stack traces
        short_hash = hashlib.shake_256(pex_tag.encode("utf-8")).hexdigest(6)
        venv_root = os.getenv("VENVS_ROOT", "/venvs")
        return os.path.join(venv_root, short_hash)

    def install_venv(
        self, venv_dir: str, source_pex_filepath: str, deps_pex_filepaths: List[str]
    ) -> bool:
        # Installs all the deps and source pex packages into a venv at venv_dir
        pex_path = ":".join(deps_pex_filepaths)
        try:
            subprocess.check_output(
                ["pex-tools", source_pex_filepath, "venv", venv_dir],
                stderr=subprocess.STDOUT,
                env={**os.environ, "PEX_PATH": pex_path},
            )
        except subprocess.CalledProcessError as err:
            shutil.rmtree(venv_dir, ignore_errors=True)  # don't leave invalid dir behind
            logging.exception(
                "Failure to unpack pex files %r, %r into a venv: %r",
                source_pex_filepath,
                pex_path,
                err.output,
            )
            return False
        logging.info(
            "Unpacked pex files %r, %r into venv at %r",
            source_pex_filepath,
            deps_pex_filepaths,
            venv_dir,
        )
        return True

    def get_working_dir_for_pex(self, pex_path: str) -> Optional[str]:
        # A special 'working_directory' package may be included the source package.
        # If so we use site-packages/working_directory/root as the working dir.
        # This allows shipping arbitrary files to the server - also used for python_file support.
        if pex_path in self.working_dirs:
            return self.working_dirs[pex_path]
        try:
            working_dir_file = subprocess.check_output(
                [
                    pex_path,
                    "-c",
                    "import working_directory; print(working_directory.__file__);",
                ],
                encoding="utf-8",
            ).strip()
            if working_dir_file:
                package_dir, _ = working_dir_file.rsplit("/", 1)  # remove trailing __init__.py
                working_dir = os.path.join(package_dir, "root")
                self.working_dirs[pex_path] = working_dir
                return working_dir
            return None

        except subprocess.CalledProcessError:
            # working_directory package is optional, just log a message
            logging.info("Cannot import working_directory package - not setting current directory.")
            return None
        except OSError:
            # some issue with pex not being runnable, log an error but don't fail yet
            # might fail later if we try to run this again
            logging.exception(
                "Ignoring failure to run pex file to determine working_directory %r", pex_path
            )
            return None
