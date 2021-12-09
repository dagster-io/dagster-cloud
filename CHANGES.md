# Changelog

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

