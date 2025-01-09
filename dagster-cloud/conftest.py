def pytest_addoption(parser):
    parser.addoption("--force-shard1", action="store_true", default=False, help="Enable sharding")
    parser.addoption(
        "--force-batching", action="store_true", default=False, help="Enable tick batching"
    )


pytest_plugins = [
    "dagster_cloud_test_infra.storage_fixtures",
]
