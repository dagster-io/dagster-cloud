# Dagster Cloud Changelog

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

