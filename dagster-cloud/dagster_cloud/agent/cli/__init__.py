import json
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

import yaml
from dagster._core.errors import DagsterHomeNotSetError
from dagster._utils.interrupts import capture_interrupts
from dagster._utils.log import default_date_format_string, default_format_string
from dagster._utils.yaml_utils import load_yaml_from_globs
from dagster_cloud_cli import ui
from typer import Argument, Option, Typer

from dagster_cloud.agent.dagster_cloud_agent import DagsterCloudAgent
from dagster_cloud.instance import DagsterCloudAgentInstance

app = Typer(help="Interact with the Dagster Cloud agent.")


def agent_home_exception():
    dagster_home_loc = (
        f"No Dagster config provided in specified directory {os.getenv('DAGSTER_HOME')}. "
        "You must specify the location of a directory containing a dagster.yaml "
        "file as a parameter or by setting the DAGSTER_HOME environment variable."
        if os.getenv("DAGSTER_HOME")
        else (
            "No directory provided or DAGSTER_CLOUD environment variable set. "
            "You must supply the location of a directory containing a dagster.yaml "
            "file as a parameter or by setting the DAGSTER_HOME environment variable."
        )
    )
    return ui.error(f"No Dagster config found.\n\n{dagster_home_loc}")


def run_local_agent(agent_logging_config_path):
    try:
        with DagsterCloudAgentInstance.get() as instance:
            logging.basicConfig(
                level=logging.INFO,
                format=default_format_string(),
                datefmt=default_date_format_string(),
                handlers=[logging.StreamHandler()],
            )

            if agent_logging_config_path:
                agent_logging_config = {
                    "version": 1,
                    "disable_existing_loggers": False,
                    **load_yaml_from_globs(str(agent_logging_config_path)),
                }
                logging.config.dictConfig(agent_logging_config)

            user_code_launcher = instance.user_code_launcher
            user_code_launcher.start()

            with DagsterCloudAgent() as agent:
                agent.run_loop(instance, user_code_launcher, agent_uuid=instance.instance_uuid)
    except DagsterHomeNotSetError:
        raise agent_home_exception()


def run_local_agent_in_environment(
    dagster_home: Optional[Path], agent_logging_config_path: Optional[Path]
):
    with capture_interrupts():
        old_env = None
        try:
            old_env = dict(os.environ)
            if dagster_home:
                os.environ["DAGSTER_HOME"] = str(dagster_home.resolve())
            run_local_agent(agent_logging_config_path)
        finally:
            os.environ.clear()
            if old_env is not None:
                os.environ.update(old_env)


DAGSTER_YAML_TEMPLATE = """
instance_class:
  module: dagster_cloud.instance
  class: DagsterCloudAgentInstance

dagster_cloud_api:
  agent_token: {token}
  deployment: {deployment}
  {agent_label_entry}

user_code_launcher:
  module: {user_code_launcher_module}
  class: {user_code_launcher_class}
  {user_code_launcher_config_entry}
"""


def run_local_agent_in_temp_environment(
    agent_token: str,
    deployment: str,
    agent_label: Optional[str],
    user_code_launcher_module: str,
    user_code_launcher_class: str,
    user_code_launcher_config: Optional[str],
):
    with TemporaryDirectory() as d:
        config_entry = ""
        if user_code_launcher_config:
            try:
                unindented_config_entry: str = yaml.dump(
                    {"config": json.loads(user_code_launcher_config)}
                )
                config_entry = unindented_config_entry.replace("\n", "\n  ")
            except json.JSONDecodeError as e:
                raise ui.error(f"Invalid User Code Launcher config JSON:\n{e}")

        with open(os.path.join(d, "dagster.yaml"), "w", encoding="utf8") as f:
            f.write(
                DAGSTER_YAML_TEMPLATE.format(
                    token=agent_token,
                    deployment=deployment,
                    agent_label_entry=f"agent_label: {agent_label}" if agent_label else "",
                    user_code_launcher_module=user_code_launcher_module,
                    user_code_launcher_class=user_code_launcher_class,
                    user_code_launcher_config_entry=config_entry,
                )
            )
        run_local_agent_in_environment(Path(d), None)


PROCESS_USER_CODE_LAUNCHER = "dagster_cloud.workspace.user_code_launcher.ProcessUserCodeLauncher"


@app.command(
    help=(
        "Runs the Dagster Cloud agent. The agent can either be run ephemerally by specifying an"
        " agent token and deployment name as CLI options, or the agent can pull its config from a"
        " dagster.yaml file. To use a dagster.yaml file, either pass a directory containing the"
        " file as a CLI argument or set the DAGSTER_HOME environment variable."
    ),
    short_help="Run the Dagster Cloud agent.",
)
def run(
    dagster_home: Optional[Path] = Argument(None),
    agent_token: str = Option(
        None, "--agent-token", "-a", help="Agent token, if running ephemerally."
    ),
    deployment: str = Option(
        None, "--deployment", "-d", help="Deployment, if running ephemerally."
    ),
    agent_label: str = Option(
        None, "--agent-label", "-l", help="Optional agent label, if running ephemerally."
    ),
    user_code_launcher: str = Option(
        PROCESS_USER_CODE_LAUNCHER,
        "--user-code-launcher",
        help="User Code Launcher to use. Defaults to the local Process User Code Launcher.",
    ),
    user_code_launcher_config: str = Option(
        None,
        "--user-code-launcher-config",
        help="Config to supply the User Code Launcher, in JSON format.",
    ),
    agent_logging_config_path: Optional[Path] = Option(
        None,
        "--agent-logging-config-path",
        help=(
            "Yaml file with logging config for the agent process that can be passed into"
            " logging.dictConfig"
        ),
        exists=True,
    ),
):
    if (
        agent_token
        or deployment
        or agent_label
        or user_code_launcher_config
        or user_code_launcher != PROCESS_USER_CODE_LAUNCHER
    ):
        if not agent_token or not deployment:
            raise ui.error("To run ephemerally, must supply both an agent token and a deployment.")
        if dagster_home:
            raise ui.error("Cannot supply both a dagster home directory and ephemeral parameters.")

        user_code_launcher_module = ""
        user_code_launcher_class = user_code_launcher
        if "." in user_code_launcher:
            user_code_launcher_module, user_code_launcher_class = user_code_launcher.rsplit(".", 1)
        run_local_agent_in_temp_environment(
            agent_token,
            deployment,
            agent_label,
            user_code_launcher_module,
            user_code_launcher_class,
            user_code_launcher_config,
        )
    else:
        run_local_agent_in_environment(dagster_home, agent_logging_config_path)
