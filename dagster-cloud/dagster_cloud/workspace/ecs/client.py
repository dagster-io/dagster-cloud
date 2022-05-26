import json
import time
import uuid
from typing import List, Optional

import boto3
import dagster._check as check
from dagster.utils import merge_dicts

from ..user_code_launcher import DAGSTER_SANDBOX_PORT_ENV
from .service import Service


class EcsServiceError(Exception):
    def __init__(self, task_arn, logs):
        message = f"ECS service failed because task {task_arn} failed:\n{logs}"
        super().__init__(message)


class Client:
    def __init__(
        self,
        cluster_name: str,
        service_discovery_namespace_id: str,
        log_group: str,
        execution_role_arn: str,
        subnet_ids: Optional[List[str]] = None,
        security_group_ids: Optional[List[str]] = None,
        ecs_client=None,
        wait_for_service: bool = True,
    ):
        self.ecs = ecs_client if ecs_client else boto3.client("ecs")
        self.logs = boto3.client("logs")
        self.service_discovery = boto3.client("servicediscovery")
        self.ec2 = boto3.resource("ec2")

        self.cluster_name = cluster_name.split("/")[-1]
        self.subnet_ids = check.opt_list_param(subnet_ids, "subnet_ids")
        self.security_group_ids = check.opt_list_param(security_group_ids, "security_group_ids")
        self.service_discovery_namespace_id = check.str_param(
            service_discovery_namespace_id, "service_discovery_namespace_id"
        )
        self.log_group = check.str_param(log_group, "log_group")
        self.execution_role_arn = check.str_param(execution_role_arn, "execution_role_arn")
        self.wait_for_service = check.bool_param(wait_for_service, "wait_for_service")

    @property
    def namespace(self):
        namespace = (
            self.service_discovery.get_namespace(
                Id=self.service_discovery_namespace_id,
            )
            .get("Namespace")
            .get("Name")
        )
        return namespace

    @property
    def taggable(self):
        settings = self.ecs.list_account_settings(
            name="serviceLongArnFormat",
            effectiveSettings=True,
        )
        return settings["settings"][0]["value"] == "enabled"

    @property
    def network_configuration(self):
        network_configuration = {
            "awsvpcConfiguration": {
                "subnets": self.subnet_ids,
                "assignPublicIp": self._assign_public_ip(),
            },
        }

        if self.security_group_ids:
            network_configuration["awsvpcConfiguration"]["securityGroups"] = self.security_group_ids

        return network_configuration

    def register_task_definition(
        self, name, image, command, task_role_arn=None, env=None, secrets=None
    ):
        if not env:
            env = {}

        family = name
        container_name = name

        secrets_dict = (
            {"secrets": [{"name": key, "valueFrom": value} for key, value in secrets.items()]}
            if secrets
            else {}
        )

        kwargs = dict(
            family=family,
            requiresCompatibilities=["FARGATE"],
            networkMode="awsvpc",
            containerDefinitions=[
                merge_dicts(
                    {
                        "name": container_name,
                        "image": image,
                        "environment": [
                            {"name": key, "value": value} for key, value in env.items()
                        ],
                        "command": command,
                        "logConfiguration": {
                            "logDriver": "awslogs",
                            "options": {
                                "awslogs-group": self.log_group,
                                "awslogs-region": self.ecs.meta.region_name,
                                "awslogs-stream-prefix": family,
                            },
                        },
                    },
                    secrets_dict,
                )
            ],
            executionRoleArn=self.execution_role_arn,
            # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
            cpu="256",
            memory="512",
        )

        if task_role_arn:
            kwargs.update(dict(taskRoleArn=task_role_arn))

        task_definition_arn = (
            self.ecs.register_task_definition(**kwargs)
            .get("taskDefinition")
            .get("taskDefinitionArn")
        )

        return task_definition_arn

    def create_service(
        self,
        name,
        image,
        command,
        task_role_arn=None,
        env=None,
        tags=None,
        register_service_discovery=True,
        secrets=None,
    ):
        # Append a unique suffix to the service name so we can do blue/green
        # deploys without setting up a load balancer.
        service_name = f"{name}_{uuid.uuid4().hex[:7]}"

        # Register a task definition
        task_definition_arn = self.register_task_definition(
            name=name,
            image=image,
            task_role_arn=task_role_arn,
            command=command,
            env=merge_dicts(env or {}, {"DAGSTER_SERVER_NAME": service_name}),
            secrets=secrets,
        )

        service_registry_arn = None
        # Configure service discovery
        if register_service_discovery:
            service_registry_arn = self._create_service_registry(
                service_name=service_name,
                tags=tags,
            )

        # Create the service
        service = self._create_service(
            service_name=service_name,
            service_registry_arn=service_registry_arn,
            task_definition_arn=task_definition_arn,
            tags=tags,
        )

        if self.wait_for_service:
            # Poll until the service is available
            self._wait_for_service(service)

        return service

    def delete_service(self, service):
        # Reduce running tasks to 0
        self.ecs.update_service(
            cluster=self.cluster_name,
            service=service.name,
            desiredCount=0,
        )
        self.ecs.get_waiter("services_stable").wait(
            cluster=self.cluster_name,
            services=[service.name],
            WaiterConfig={"Delay": 1, "MaxAttempts": 300},
        )

        # Delete the ECS service
        self.ecs.delete_service(
            cluster=self.cluster_name,
            service=service.name,
        )
        self.ecs.get_waiter("services_inactive").wait(
            cluster=self.cluster_name,
            services=[service.name],
            WaiterConfig={"Delay": 1, "MaxAttempts": 300},
        )

        # Delete service discovery
        service_discovery_id = self._get_service_discovery_id(
            service.hostname,
        )
        if service_discovery_id:
            self.service_discovery.delete_service(
                Id=service_discovery_id,
            )

    def list_services(self):
        services = []

        paginator = self.ecs.get_paginator("list_services")
        for page in paginator.paginate(cluster=self.cluster_name):
            for arn in page.get("serviceArns"):
                service = Service(client=self, arn=arn)
                services.append(service)

        return services

    def run_task(self, name, image, command):
        task_definition_arn = self.register_task_definition(
            name=name,
            image=image,
            command=command,
        )

        task_arn = (
            self.ecs.run_task(
                taskDefinition=task_definition_arn,
                cluster=self.cluster_name,
                launchType="FARGATE",
                networkConfiguration=self.network_configuration,
            )
            .get("tasks", [{}])[0]
            .get("taskArn")
        )

        self.ecs.get_waiter("tasks_stopped").wait(
            cluster=self.cluster_name,
            tasks=[task_arn],
            WaiterConfig={"Delay": 1, "MaxAttempts": 300},
        )

        exit_code = (
            self.ecs.describe_tasks(
                cluster=self.cluster_name,
                tasks=[task_arn],
            )
            .get("tasks", [{}])[0]
            .get("containers", [{}])[0]
            .get("exitCode")
        )

        if exit_code:
            raise Exception(self._get_logs(task_arn))

        return True

    def _create_service_registry(self, service_name, tags=None):
        if not tags:
            tags = {}

        # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-service-discovery.html
        service_registry_arn = (
            self.service_discovery.create_service(
                Name=service_name,
                NamespaceId=self.service_discovery_namespace_id,
                DnsConfig={
                    "DnsRecords": [
                        {"Type": "A", "TTL": 60},
                    ]
                },
                Tags=[{"Key": key, "Value": value} for key, value in tags.items()],
            )
            .get("Service", {})
            .get("Arn")
        )
        return service_registry_arn

    def _create_service(self, service_name, task_definition_arn, service_registry_arn, tags=None):
        params = dict(
            cluster=self.cluster_name,
            serviceName=service_name,
            taskDefinition=task_definition_arn,
            launchType="FARGATE",
            desiredCount=1,
            networkConfiguration=self.network_configuration,
        )

        if service_registry_arn:
            params["serviceRegistries"] = [{"registryArn": service_registry_arn}]

        if tags and self.taggable:
            params["tags"] = [{"key": key, "value": value} for key, value in tags.items()]

        arn = self.ecs.create_service(**params).get("service").get("serviceArn")

        return Service(client=self, arn=arn)

    def _wait_for_service(self, service):
        service_name = service.name
        messages = []
        start_time = time.time()
        while start_time + 300 > time.time():
            response = self.ecs.describe_services(
                cluster=self.cluster_name,
                # We only expect our API call to describe one service
                services=[service_name],
            )
            if response.get("services"):
                service = response.get("services")[0]
                messages = [event.get("message") for event in service.get("events")]
                # Poll until at least 1 task to starts instead of at least 1 task failing
                # This is because IAM is eventually consistent so the first event or two
                # will sometimes fail with "ECS was unable to assume the role" but will
                # resolve itself with enough time:
                # https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_eventual-consistency
                if any(["has started 1 tasks" in message for message in messages]):
                    return self._wait_for_task(service_name)
                time.sleep(1)
            elif response.get("failures"):
                failures = response.get("failures")
                raise Exception(f"ECS DescribeTasks API returned failures: {json.dumps(failures)}")
            else:
                # This might not be a possible state; it's unclear if the ECS API can return empty lists for both
                # "failures" and "services":
                # https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_DescribeServices.html
                raise Exception(
                    f"ECS DescribeTasks API returned an empty response for service {service.arn}."
                )
        raise Exception(messages)

    def _wait_for_task(self, service_name):
        # Check if a task can start
        start_time = time.time()
        while start_time + 300 > time.time():
            running = self.ecs.list_tasks(
                cluster=self.cluster_name,
                serviceName=service_name,
                desiredStatus="RUNNING",
            ).get("taskArns")

            stopped = self.ecs.list_tasks(
                cluster=self.cluster_name,
                serviceName=service_name,
                desiredStatus="STOPPED",
            ).get("taskArns")

            if stopped:
                logs = []
                task_arn = stopped[0]

                try:
                    logs = self._get_logs(task_arn)
                except:
                    pass

                if logs:
                    raise EcsServiceError(task_arn=task_arn, logs=logs)

                stopped_reason = (
                    self.ecs.describe_tasks(
                        cluster=self.cluster_name,
                        tasks=[task_arn],
                    )
                    .get("tasks")[0]
                    .get("stoppedReason")
                )
                raise EcsServiceError(task_arn=task_arn, logs=stopped_reason)

            if running:
                task = self.ecs.describe_tasks(cluster=self.cluster_name, tasks=running,).get(
                    "tasks"
                )[0]

                if task.get("lastStatus") == "RUNNING":
                    return True

            time.sleep(1)
        raise Exception(f"Timed out waiting for tasks to start for service: {service_name}")

    def _get_service_discovery_id(self, hostname):
        service_name = hostname.split("." + self.namespace)[0]

        paginator = self.service_discovery.get_paginator("list_services")
        for page in paginator.paginate(
            Filters=[
                {
                    "Name": "NAMESPACE_ID",
                    "Values": [
                        self.service_discovery_namespace_id,
                    ],
                    "Condition": "EQ",
                },
            ],
        ):
            for service in page["Services"]:
                if service["Name"] == service_name:
                    return service["Id"]

    def _assign_public_ip(self):
        # https://docs.aws.amazon.com/AmazonECS/latest/userguide/fargate-task-networking.html
        # Assign a public IP if any of the subnets are public
        route_tables = self.ec2.route_tables.filter(
            Filters=[
                {"Name": "association.subnet-id", "Values": self.subnet_ids},
            ]
        )

        # Consider a subnet to be public if it has a route that targets
        # an internet gateway; private subnets have routes that target NAT gateways
        for route_table in route_tables:
            if any(route.nat_gateway_id for route in route_table.routes):
                return "DISABLED"
        return "ENABLED"

    def _get_logs(self, task_arn):
        task = self.ecs.describe_tasks(cluster=self.cluster_name, tasks=[task_arn]).get("tasks")[0]

        task_definition_arn = task.get("taskDefinitionArn")
        task_definition = self.ecs.describe_task_definition(taskDefinition=task_definition_arn).get(
            "taskDefinition"
        )

        container_definition = task_definition.get("containerDefinitions")[0]
        log_stream_prefix = (
            container_definition.get("logConfiguration").get("options").get("awslogs-stream-prefix")
        )
        container_name = container_definition.get("name")
        task_id = task_arn.split("/")[-1]

        log_stream = f"{log_stream_prefix}/{container_name}/{task_id}"

        events = self.logs.get_log_events(
            logGroupName=self.log_group,
            logStreamName=log_stream,
        ).get("events")

        return [event.get("message") for event in events]

    def list_allocated_sandbox_ports(self) -> List[int]:
        allocated_ports = []
        for service in self.list_services():
            if "dagster/location_name" in service.tags.keys():
                port = service.environ.get(DAGSTER_SANDBOX_PORT_ENV)
                if port:
                    allocated_ports.append(int(port))
        return allocated_ports
