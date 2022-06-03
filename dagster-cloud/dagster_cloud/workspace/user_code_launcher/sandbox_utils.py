# pylint: disable=protected-access
import time

from dagster_cloud_backend.settings.organization_settings import InternalOrganizationSettingsKeys


def allocate_ssh_port(instance, ttl=300):
    # While we pilot sandboxes, each organization shares a single frps server.
    # The agent needs to know which port to assign to the frpc client when it
    # launches a Sandbox. So we need a way to coordinate port allocations
    # "globally" for an organization (not just within a single agent).
    #
    # This is a temporary solution; eventually we'll run a distinct frps server
    # per location so we'll be able to remove all of this.
    #
    # 1. Check Postgres to see which ports are known to be in use
    # 2. Use Redis as a temporary "lock" for ports that have been allocated but
    #    may not show up in Postgres yet (because reconcilliation is ongoing)
    # 3. Return a port that doesn't exist in either set
    settings = instance.load_organization_settings().internal_settings.get(
        InternalOrganizationSettingsKeys.ENABLE_SANDBOX_REVERSE_PROXY.value
    )
    if not settings:
        return None

    min_port = int(settings["port"]) + 1
    max_port = min_port + int(settings["max_supported_locations"])
    potential_ports = range(min_port, max_port)

    # Hack: ideally this would live in a storage layer somewhere but I'm keeping
    # it here to make it easy to rip out when we don't need it anymore.
    redis = instance.user_cloud_agent_request_storage._redis_client
    key = f"SANDBOX_PORTS_LOCK_ORG_{instance.cloud_deployment.organization_id}"

    # We store everything in a sorted set (scored by expiration timestamp)
    # Remove expired port locks ("scores" from the beginning of time up until now)
    redis.zremrangebyscore(key, "-inf", time.time())

    # Find an available port
    allocated_ports = instance.cloud_storage.get_allocated_sandbox_ports()
    locked_ports = [int(port) for port in redis.zrange(key, 0, -1)]
    try:
        port = next(
            port
            for port in potential_ports
            if port not in locked_ports and port not in allocated_ports
        )
    except:
        raise Exception("All available SSH ports are already allocated.")

    # Lock our port
    expire_at = time.time() + ttl
    redis.zadd(key, {port: expire_at})

    return port
