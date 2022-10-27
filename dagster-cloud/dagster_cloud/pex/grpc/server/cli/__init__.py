import logging
import os
from typing import Optional

from dagster._core.utils import coerce_valid_log_level
from typer import Option, Typer, echo

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
