import logging
import os
from typing import Optional

from dagster._core.utils import coerce_valid_log_level
from typer import Option, Typer, echo

from ..grpc.server import run_multipex_server

app = Typer(hidden=True)


@app.command(short_help="Run multi-pex server to spin up subprocesses via PEX files")
def grpc(
    host: str = Option(default="localhost"),
    port: int = Option(default=...),
    max_workers: Optional[int] = Option(default=None),
):
    run_multipex_server(port=port, host=host, print_fn=echo, max_workers=max_workers)
