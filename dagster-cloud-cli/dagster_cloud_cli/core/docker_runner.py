import os
import subprocess
from typing import Dict, List, Optional

from dagster_cloud_cli import ui

DAGSTER_CLOUD_BUILDER_IMAGE = "ghcr.io/dagster-io/dagster-manylinux-builder:latest"


def run_dagster_cloud(
    map_folders: Dict[str, str], run_args: List[str], env: Dict[str, Optional[str]]
) -> subprocess.CompletedProcess:
    proc = run_docker_image(
        image_name=os.getenv("DAGSTER_CLOUD_BUILDER_IMAGE", DAGSTER_CLOUD_BUILDER_IMAGE),
        map_folders=map_folders,
        env=env,
        entrypoint="dagster-cloud",
        run_args=run_args,
    )
    return proc


def run_docker_image(
    image_name,
    map_folders: Dict[str, str],
    env: Dict[str, Optional[str]],
    entrypoint: str,
    run_args: List[str],
) -> subprocess.CompletedProcess:
    mount_args = []
    for target_folder, source_folder in map_folders.items():
        mount_args.extend(["--mount", f"type=bind,source={source_folder},target={target_folder}"])

    env_args = []
    for env_name, env_value in env.items():
        if env_value is None:
            # specifying just the name copies the env from the calling environment
            env_args.extend(["--env", env_name])
        else:
            env_args.extend(["--env", f"{env_name}={env_value}"])

    ui.print(f"Running docker {image_name}")
    ui.print("Mapped folders:")
    for target_dir, source_dir in map_folders.items():
        ui.print(f" - {source_dir} -> {target_dir}")

    cmd = [
        "docker",
        "run",
        "--platform=linux/amd64",
        *env_args,
        *mount_args,
        "--entrypoint",
        entrypoint,
        image_name,
        *run_args,
    ]
    return subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, check=False)
