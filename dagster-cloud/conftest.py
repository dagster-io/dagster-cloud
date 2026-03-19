import pytest


def pytest_addoption(parser: pytest.Parser):
    parser.addoption("--force-shard1", action="store_true", default=False, help="Enable sharding")


pytest_plugins = [
    "dagster_cloud_test_infra.instance_fixtures",
]
