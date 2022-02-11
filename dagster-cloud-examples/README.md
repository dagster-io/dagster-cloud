# Example Code

This directory contains sample repos that are shipped with the `dagster-cloud-examples` package.

They can be installed on the local agent, or installed from built Docker packages on other agents.

## Using Sample Code on Local Agent

To add a specific code sample to the local agent environment, run:

```bash
dagster-cloud workspace add-location cloud_cereals_example --package-name dagster_cloud_examples.cereals
```

Or, to load all examples:

```bash
dagster-cloud workspace add-location cloud_examples --package-name dagster_cloud_examples
```

## Using Sample Code on ECS/Docker/K8s Agent

To add a specific code sample to an image-dependent agent type, run:

```bash
dagster-cloud workspace add-location cloud_cereals_example --image dagster/dagster-cloud-examples --package-name dagster_cloud_examples.cereals
```

Or, to load all examples:

```bash
dagster-cloud workspace add-location cloud_examples --image dagster/dagster-cloud-examples --package-name dagster_cloud_examples
```
