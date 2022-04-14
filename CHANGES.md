# Dagster Cloud Changelog

# 0.14.9

### New

- You can now set a `container_context` key in your code locations in the Workspace tab, which lets you set configuration for a specific execution environment (K8s, Docker, or ECS) for that code location. Previously, this configuration could only be set in the `dagster.yaml` file for your agent, which required you to share it across code locations, and restart your agent whenever it changed. For example, you can specify that a code location should include a secret called `my_secret` and run in a K8s namespace called `my_namespace` whenever the Kubernetes agent creates a pod for that location:

```
location_name: test-location
image: dagster/dagster-cloud-template:latest
code_source:
  package_name: dagster_cloud_template
container_context:
  k8s:
    namespace: my_namespace
    env_secrets:
      - my_secret
```

For more information, see https://docs.dagster.cloud/guides/adding-code#environment-specific-config.

- [Alert policies](https://docs.dagster.cloud/guides/alerts) can now be viewed in Dagit. You can see the list of alert policies in a tab under Cloud Settings.

# 0.14.8

### New

- Dagster Cloud Slack notifications now display message previews.

### Bugfixes

- Fixed an issue where event logs displayed that a Slack notification had failed when they actually succeeded.

# 0.14.7

### New

- Email alerts now include the run’s start and end time.
- Dagster Cloud now displays more verbose event logging when an alert succeeds or fails.
- Added links to Dagster Cloud changelog and status page in the login page.

### Bugfixes

- Fixed an issue where alert policy names were required to be unique across deployments in an organization.

### Documentation

- Added instructions to display your alert policies using the Dagster Cloud CLI.

# 0.14.6

### New

- Added the ability to add or update single code locations with a YAML file in the dagster-cloud CLI. For more information, see the adding code to Dagster Cloud docs (https://docs.dagster.cloud/guides/adding-code#using-the-dagster-cloud-cli).
- When Dagster Cloud is temporarily unavailable due to scheduled maintenance, jobs that are running when the maintenance starts will wait for the maintenance to conclude and then continue execution, instead of failing.
- Alert policies are now supported in Dagster Cloud. Alert policies define which jobs will trigger an alert, the conditions under which an alert will be sent, and how the alert will be sent.

  An alert policy includes a set of configured tags. Only jobs that contain all the tags for a given alert policy are eligible for that alert. Additionally, an alert policy includes a set of conditions under which an alert will be triggered. For example, the alert can be triggered to fire on job failure, job success, or both events. Finally, an alert policy specifies where the alert will be sent. Currently, we support Slack and email as targets for an alert.

  See https://docs.dagster.cloud/guides/alerts for more details.

# 0.14.4

### New

- You can now [set secrets with the ECS Agent using the same syntax that you use to set secrets in the ECS API](<(https://docs.dagster.io/0.14.4/deployment/guides/aws#secrets-management-in-ecs)>).
- The ECS Agent now raises the underlying ECS API failure if it cannot successfully start a task or service.
- You can now delete a code location without running an agent.

Bugfixes

- Fixed a Python packaging issue which caused the `dagster_cloud_examples` package to fail to load when used with the local agent.

Documentation

- Document a strategy for developing your Dagster jobs locally using Dagster Cloud and the DockerUserCodeLauncher.
- Document how to grant AWS IAM permissions to Dagster K8s pods using service accounts.

# 0.14.3

### New

- [K8s] The agent now monitors active runs. Failures in the underlying Kubernetes Job (e.g. an out of memory error) will now be reported in Dagit.

### Bugfixes

- Fixed an issue where the favicon didn’t update to reflect success, pending, or failure status when looking at a job’s run page.

# 0.14.2

### New

- Individual `context.log` messages which appear in the Dagster event log will now be truncated after 50,000 characters. The full contents of these messages remain available in the compute logs tab. For large logs, we recommend logging straight to stdout or stderr rather than using `context.log`.

### Bugfixes

- Added a missing IAM permission to the ECS Agent Cloudformation template that was preventing the ECS agent from being able to terminate runs.

### Documentation

- Added a new [guide](https://docs.dagster.cloud/guides/adding-code) to the Dagster Cloud docs covering how to add and update code.

# 0.14.1

### Bugfixes

- Sensors that have a default status can now be manually started. Previously, this would fail with an invariant exception.

# 0.14.0

### New

- Added a button to test out Dagster Cloud with a sample code location when you go to the Workspace tab on an empty Deployment.
- Added quick links to log in to your organizations from https://dagster.cloud (must be signed in)
- [K8s] Added an `imagePullGracePeriod` field to the helm chart that tells the agent how long to allow errors while pulling an image before failing. For example:

```yaml
# values.yaml
workspace:
  imagePullGracePeriod: 60
```

- [K8s] The agent container can now run as a non-root user. The `docker.io/dagster/dagster-cloud-agent` image includes a dagster user with ID `1001` that can be assumed by setting `podSecurityContext` in your Helm values:

```yaml
# values.yaml
podSecurityContext:
  runAsUser: 1001
```

### Bugfixes

- Fixed an issue where run status sensors sometimes failed to trigger.
- [ECS] Fixed an issue where the Cloudformation template for setting up an ECS agent would sometimes fail to spin up tasks with an AWS Secretsmanager permission error.
- Fixed an issue where the agent sometimes left old user code servers running after the server failed to start up.
- Fixed an issue where the agent sometimes stopped sending heartbeats while it was in the middle of starting up a new user code server.
- Fixed an issue where an erroring code location would not show as updating when redeploying.

### Documentation

- Added a new guide to configuring the Kubernetes agent (https://docs.dagster.cloud/agents/kubernetes/configuring).
- Updated documentation to show adding new code locations through Dagit.

# 0.13.19

### New

- The Dagster Cloud workspace page now allows creating, deleting, and updating code locations from Cloud Dagit in addition to via the CLI.
- The ECS agent can now override the `secrets_tag` parameter to None, which will cause it to not look for any secrets to be included in the tasks that the agent creates. This can be useful in situations where the agent does not have permissions to query AWS Secretsmanager
- Added a `dagit_url` property to the DagsterInstance in Dagster Cloud that can be used to reference the Dagster Cloud Dagit URL within ops and sensors.
- Introduced a path to run the local Dagster Cloud agent ephemerally without specifying a `dagster.yaml`, by using CLI arguments.
- Added the ability to configure [Python logging](https://docs.dagster.io/concepts/logging/python-logging#python-logging) in the Kubernetes agent helm chart. For example:

```
pythonLogs:
   # The names of python loggers that will be captured as Dagster logs
   managedPythonLoggers:
     - foo_logger
   # The log level for the instance. Logs emitted below this severity will be ignored.
   # One of [NOTSET, DEBUG, INFO, WARNING, WARN, ERROR, FATAL, CRITICAL]
   pythonLogLevel: INFO
   # Python log handlers that will be applied to all Dagster logs
   dagsterHandlerConfig:
     handlers:
       myHandler:
         class: logging.FileHandler
         filename: "/logs/my_dagster_logs.log"
         mode: "a"
```

### Bugfixes

- Added a missing AWS secretsmanager permission to the example CloudFormation template for creating an ECS agent.
- Improved dagster-cloud CLI web authentication error messages on the client and server, including troubleshooting steps and instructions on alternative token authentication.
- Fixed an issue where deleting a deployment from Dagit would sometimes fail.

### Documentation

- Added a setup guide for SAML SSO using PingOne.
- Added a documentation page detailing switching, creating, and deleting deployments.

# 0.13.18

### New

- Secrets in the ECS Agent: When using Dagster Cloud in ECS, you can specify a list of AWS Secrets Manager ARNs to include in all tasks that the agent spins up. Any secrets that are tagged with the key “dagster” in AWS Secrets Manager (or a custom key that you specify) will also be included in all tasks. You can customize the secrets in your ECS Agent in your `dagster.yaml` file as follows:

```
user_code_launcher:
  module: dagster_cloud.workspace.ecs
  class: EcsUserCodeLauncher
  config:
    cluster: your-cluster-name
    subnets:
      - your-subnet-name
    service_discovery_namespace_id: your-service-discovery-namespace-id
    execution_role_arn: your-execution-role-arn
    log_group: your-log-group
    secrets_tag: "my-tag-name"
    secrets:- "arn:aws:secretsmanager:us-east-1:1234567890:secret:MY_SECRET"`
```

- Added support in Dagster Cloud for disabling compute logs, which display the stdout/stderr output from your Dagster jobs within Dagit. When using the Dagster Cloud helm chart, by setting `Values.computeLogs.enabled` to false, users can prevent compute logs from being forwarded into Dagster Cloud. Logging configured separately from Dagster on Kubernetes will continue to work, but won’t be viewable in the Dagster Cloud UI.

### Bugfixes

- Fixed an issue where omitting the Dagster Cloud agent endpoint when installing the Kubernetes agent using the helm chart would sometimes cause the agent to fail to start. The agent endpoint is no longer a required field on the helm chart.
- Fixed an issue with dagster-cloud CLI web authentication where users who did not have an available user token could not be authenticated.

### Documentation

- Added a “Customizing your agent” section to the docs, including documentation on how to disable compute logging when manually authoring an agent’s `dagster.yaml`.

# 0.13.17

### New

- Dagster Cloud now supports authenticating with Github. Verified emails associated with your Github user can be used for login authorization for Dagster Cloud.
- Various improvements to the Dagster Cloud CLI.
  - You can now display your version of Dagster Cloud by invoking `dagster-cloud --version`.
  - Enabled `-h` as another alias for `--help`
  - If you’ve configured a default organization, deployment, and token using `dagster-cloud config setup`, your default values will now show in the help text for any command.
  - Installing completions using `--install-completion` no longer requires you to pass the name of your shell.

### Documentation

- The Dagster Cloud tutorial at https://docs.dagster.cloud has been streamlined and rewritten.
- Added documentation for running multiple agents for the same Dagster Cloud deployment at https://docs.dagster.cloud/deployment/multiple-agents.
- Added documentation for installing completions to the dagster-cloud CLI at https://docs.dagster.cloud/dagster-cloud-cli#completions.

# 0.13.16

## Dagster Cloud

### New

- `dagster-cloud config setup` now allows the user to authenticate the CLI by logging in through the browser.

### Bugfixes

- When uploading SAML metadata via the `dagster-cloud` CLI, a deployment no longer needs to be specified.

## Agent

### New

- If your agent’s Dagster Cloud version is >=0.13.15, its version will now surface on your Dagster Cloud instance status page.
- The Kubernetes agent can now specify a dictionary of labels that will be applied to the pods spun up by the agent. Here is an example `values.yaml` snippet that adds pod labels:

  ```
  workspace:
    labels:
      my_label_key: my_label_value
  ```

# 0.13.14

## Dagster Cloud

### New

- Deployment settings (run queue configuration, run start timeouts) can now be configured via the `dagster-cloud` CLI. For example: `dagster-cloud deployment settings set-from-file example-settings.yaml`. These settings will soon be available to configure within Dagit as well as the CLI.

  ```
  # example-settings.yaml
  run_queue:
    max_concurrent_runs: 10
    tag_concurrency_limits: []
  run_monitoring:
    start_timeout_seconds: 300
  ```

- There is now documentation to set up SAML SSO for Google Workspace. See https://docs.dagster.cloud/auth/google-workspace for details.

## Agent

### New

- Containers created by the Docker agent can now be configured with any configuration that’s available on the [`Container.run` call in the Docker Python client](https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run). For example, to ensure that containers are automatically removed when they are finished running, you can configure your `dagster.yaml` as follows:

  ```
  user_code_launcher:
    module: dagster_cloud.workspace.docker
    class: DockerUserCodeLauncher
    config:
      container_kwargs:
        auto_remove: true
  ```

- Logging is now emitted from the `dagster_cloud` logger.

# 0.13.13

## Dagster Cloud

### New

- Performance improvements in Dagit on the Run Viewer page, Runs page, and Workspace page.
- The Status page in Dagit now displays more information about each agent that is currently running.
- When the agent is down, the scheduler will now wait for the agent to be available again before running schedule ticks, instead of marking the tick as failed.
- The Dagster Cloud Github Action now supports creating Dagit previews of your Dagster code when you create a pull request. To set up this feature, please see the [documentation](https://docs.dagster.cloud/deployment/code-previews).
- The `dagster-cloud` CLI will now notify the user if their Dagster Cloud agent is not running when attempting to modify workspace locations, instead of polling for the agent to sync the update.
- The `dagster-cloud configure` CLI command has been renamed to `dagster-cloud config setup`.
- The default number of jobs that can run at once per deployment has in raised to 25 instead of 10. This value will become configurable per-deployment in an upcoming release.

### Bugfixes

- Fixed an issue where the Kubernetes agent would sometimes time out while creating a new code deployment on startup.
- Fixed an issue where importing local modules in job code would sometimes fail to load unless you explicitly set a working directory via the `dagster-cloud` CLI.
- Fixed an issue where switching organizations could cause a GraphQL error during the `dagster-cloud` CLI configuration process.
- Fixed a bug where trying to load compute logs in Dagit for a step that hadn’t yet finished displayed an error.

## Agent

### New

- Agents can now be optionally assigned a label with the `dagster_cloud_api.agent_label` configuration option in the `dagster.yaml` file.
- ECS agent CloudFormation templates now append part of the stack ID to the Service Discovery name to prevent naming conflicts when launching multiple agents for the same deployment.

# 0.13.12

## Agent

### New

- [ECS] Previously, ECS tasks and services created by the ECS agent always used the VPC’s default security group. Now, you can configure it to use a different list of security groups in your `dagster.yaml`.
- When configuring code locations in Dagster Cloud, you can now specify all the same configuration options that you can when specifying a workspace in open-source Dagster. Run `dagster-cloud workspace add-location --help` to see the full set of available options.

## Dagster Cloud

### Bugfixes

- Fixed an issue where the “View Configuration...” link on schedules went to an invalid URL in Dagster Cloud.
- Fixed an issue where Dagster Cloud links in the Okta store were sometimes invalid.
- Fixed an issue where some externally-launched SAML logins lacking URL parameters would cause an error.
- Fixed an issue where schedule ticks would sporadically timeout in Cloud (requires upgrading the agent to 0.13.12)
- Fixed an issue where checking the “Force Termination Immediately" checkbox in Dagit would cancel the run without attempting to clean up the computational resources created by that run.
- Fixed an issue where Dagit would sometimes require the “Force Termination Immediately” checkbox to be set when terminating a run, instead of offering it as an option.

# 0.13.11

## Dagster Cloud

### New

- Email alerts now display basic metadata about the failed run, including the number of steps succeeded, steps failed, and a link to the run in your Dagster Cloud instance.
- Improved Dagit page loading times for repositories with many schedules or sensors.
- The `dagster-cloud` CLI now correctly exits on keyboard interrupt.

## Agent

### Bugfixes

- [ECS] Ensured that the ECS agent properly cleans up ServiceDiscovery services when the agent spins down. Previously, this left orphaned services which would prevent CloudFormation teardown from deleting the associated ServiceDiscovery namespace.

## CI/CD GitHub Action

### New

- Support specifying a Docker target stage when building multistage Dockerfiles.
