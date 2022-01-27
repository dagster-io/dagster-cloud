# Dagster Cloud Changelog

# 0.13.17

### New

* Dagster Cloud now supports authenticating with Github. Verified emails associated with your Github user can be used for login authorization for Dagster Cloud.
* Various improvements to the Dagster Cloud CLI.
    * You can now display your version of Dagster Cloud by invoking `dagster-cloud --version`.
    * Enabled `-h` as another alias for `--help`
    * If you’ve configured a default organization, deployment, and token using `dagster-cloud config setup`, your default values will now show in the help text for any command.
    * Installing completions using `--install-completion` no longer requires you to pass the name of your shell.

### Documentation

* The Dagster Cloud tutorial at https://docs.dagster.cloud has been streamlined and rewritten.
* Added documentation for running multiple agents for the same Dagster Cloud deployment at https://docs.dagster.cloud/deployment/multiple-agents.
* Added documentation for installing completions to the dagster-cloud CLI at https://docs.dagster.cloud/dagster-cloud-cli#completions.

# 0.13.16

## Dagster Cloud

### New

* `dagster-cloud config setup` now allows the user to authenticate the CLI by logging in through the browser.

### Bugfixes

* When uploading SAML metadata via the `dagster-cloud` CLI, a deployment no longer needs to be specified.

## Agent

### New

* If your agent’s Dagster Cloud version is >=0.13.15, its version will now surface on your Dagster Cloud instance status page.
* The Kubernetes agent can now specify a dictionary of labels that will be applied to the pods spun up by the agent. Here is an example `values.yaml` snippet that adds pod labels:

    ```
    workspace:
      labels:
        my_label_key: my_label_value
    ```
# 0.13.14
## Dagster Cloud

### New

* Deployment settings (run queue configuration, run start timeouts) can now be configured via the `dagster-cloud` CLI. For example: `dagster-cloud deployment settings set-from-file example-settings.yaml`. These settings will soon be available to configure within Dagit as well as the CLI.

    ```
    # example-settings.yaml
    run_queue:
      max_concurrent_runs: 10
      tag_concurrency_limits: []
    run_monitoring:
      start_timeout_seconds: 300
    ```

* There is now documentation to set up SAML SSO for Google Workspace. See https://docs.dagster.cloud/auth/google-workspace for details.

## Agent

### New

* Containers created by the Docker agent can now be configured with any configuration that’s available on the [`Container.run` call in the Docker Python client](https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run). For example, to ensure that containers are automatically removed when they are finished running, you can configure your `dagster.yaml` as follows:

    ```
    user_code_launcher:
      module: dagster_cloud.workspace.docker
      class: DockerUserCodeLauncher
      config:
        container_kwargs:
          auto_remove: true
    ```

* Logging is now emitted from the `dagster_cloud` logger.

# 0.13.13

## Dagster Cloud

### New

* Performance improvements in Dagit on the Run Viewer page, Runs page, and Workspace page.
* The Status page in Dagit now displays more information about each agent that is currently running.
* When the agent is down, the scheduler will now wait for the agent to be available again before running schedule ticks, instead of marking the tick as failed.
* The Dagster Cloud Github Action now supports creating Dagit previews of your Dagster code when you create a pull request. To set up this feature, please see the [documentation](https://docs.dagster.cloud/deployment/code-previews).
* The `dagster-cloud` CLI will now notify the user if their Dagster Cloud agent is not running when attempting to modify workspace locations, instead of polling for the agent to sync the update.
* The `dagster-cloud configure` CLI command has been renamed to `dagster-cloud config setup`.
* The default number of jobs that can run at once per deployment has in raised to 25 instead of 10. This value will become configurable per-deployment in an upcoming release.

### Bugfixes

* Fixed an issue where the Kubernetes agent would sometimes time out while creating a new code deployment on startup.
* Fixed an issue where importing local modules in job code would sometimes fail to load unless you explicitly set a working directory via the `dagster-cloud` CLI.
* Fixed an issue where switching organizations could cause a GraphQL error during the `dagster-cloud` CLI configuration process.
* Fixed a bug where trying to load compute logs in Dagit for a step that hadn’t yet finished displayed an error.

## Agent

### New

* Agents can now be optionally assigned a label with the `dagster_cloud_api.agent_label` configuration option in the `dagster.yaml` file.
* ECS agent CloudFormation templates now append part of the stack ID to the Service Discovery name to prevent naming conflicts when launching multiple agents for the same deployment.

# 0.13.12

## Agent

### New

* [ECS] Previously, ECS tasks and services created by the ECS agent always used the VPC’s default security group. Now, you can configure it to use a different list of security groups in your `dagster.yaml`.
* When configuring code locations in Dagster Cloud, you can now specify all the same configuration options that you can when specifying a workspace in open-source Dagster. Run `dagster-cloud workspace add-location --help` to see the full set of available options.

## Dagster Cloud

### Bugfixes

* Fixed an issue where the “View Configuration...” link on schedules went to an invalid URL in Dagster Cloud.
* Fixed an issue where Dagster Cloud links in the Okta store were sometimes invalid.
* Fixed an issue where some externally-launched SAML logins lacking URL parameters would cause an error.
* Fixed an issue where schedule ticks would sporadically timeout in Cloud (requires upgrading the agent to 0.13.12)
* Fixed an issue where checking the “Force Termination Immediately" checkbox in Dagit would cancel the run without attempting to clean up the computational resources created by that run.
* Fixed an issue where Dagit would sometimes require the “Force Termination Immediately” checkbox to be set when terminating a run, instead of offering it as an option.

# 0.13.11

## Dagster Cloud

### New

* Email alerts now display basic metadata about the failed run, including the number of steps succeeded, steps failed, and a link to the run in your Dagster Cloud instance.
* Improved Dagit page loading times for repositories with many schedules or sensors.
* The `dagster-cloud` CLI now correctly exits on keyboard interrupt.
## Agent

### Bugfixes

* [ECS] Ensured that the ECS agent properly cleans up ServiceDiscovery services when the agent spins down. Previously, this left orphaned services which would prevent CloudFormation teardown from deleting the associated ServiceDiscovery namespace.


## CI/CD GitHub Action

### New

* Support specifying a Docker target stage when building multistage Dockerfiles.
