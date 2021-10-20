import uuid

import click
from dagster.utils.interrupts import capture_interrupts
from dagster_cloud.daemon.dagster_cloud_api_daemon import DagsterCloudApiAgent
from dagster_cloud.instance import DagsterCloudAgentInstance


@click.command(
    name="run",
    help="Run the dagster-cloud agent",
)
def run_command():
    with capture_interrupts():
        with DagsterCloudAgentInstance.get() as instance:
            if instance.is_ephemeral:
                raise Exception(
                    "dagster-cloud agent can't run using an in-memory instance. Make sure "
                    "the DAGSTER_HOME environment variable has been set correctly and that "
                    "you have created a dagster.yaml file there."
                )

            user_code_launcher = instance.user_code_launcher
            user_code_launcher.start()

            with DagsterCloudApiAgent() as agent:
                agent.run_loop(instance, user_code_launcher, agent_uuid=str(uuid.uuid4()))
