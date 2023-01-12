import os
import subprocess
from typing import Optional

from dagster._serdes import deserialize_as
from dagster._utils.interrupts import capture_interrupts
from dagster_cloud_cli.core.workspace import PexMetadata
from typer import Option, Typer, echo

from ..registry import PexS3Registry
from ..server import run_multipex_server

app = Typer(hidden=True)


@app.command(short_help="Run multi-pex server to spin up subprocesses via PEX files")
def grpc(
    host: str = Option(default="localhost"),
    port: Optional[int] = Option(default=None),
    socket: Optional[str] = Option(default=None),
    max_workers: Optional[int] = Option(default=None),
    local_pex_files_dir: Optional[str] = Option(
        default="/tmp/pex-files", envvar="LOCAL_PEX_FILES_DIR"
    ),
):
    run_multipex_server(
        port=port,
        socket=socket,
        host=host,
        print_fn=echo,
        max_workers=max_workers,
        local_pex_files_dir=local_pex_files_dir,
    )


@app.command(short_help="Execute a run with a PEX file")
def execute_run(
    input_json: str,
    pex_metadata_json: str,
    local_pex_files_dir: Optional[str] = Option(
        default="/tmp/pex-files", envvar="LOCAL_PEX_FILES_DIR"
    ),
):
    with capture_interrupts():
        pex_metadata = deserialize_as(pex_metadata_json, PexMetadata)
        executable = PexS3Registry(local_pex_files_dir).get_pex_executable(pex_metadata)

        subprocess.run(
            [
                executable.source_path,
                "-m",
                "dagster",
                "api",
                "execute_run",
                input_json,
            ],
            check=True,
            env={**os.environ.copy(), **executable.environ},
            cwd=executable.working_directory,
        )
