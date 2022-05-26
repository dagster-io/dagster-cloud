from typing import List

from dagster_cloud.api.dagster_cloud_api import DagsterCloudSandboxProxyInfo


def find_unallocated_sandbox_port(
    allocated_ports: List[int],
    proxy_info: DagsterCloudSandboxProxyInfo,
) -> str:
    """
    Multiple frpc clients can't be configured to run on the same port for the same
    frps server. Given a list of existing ports and the proxy information, find an
    unused port.
    """
    valid_ports = range(proxy_info.min_port, proxy_info.max_port + 1)

    try:
        return next(str(port) for port in valid_ports if port not in allocated_ports)
    except StopIteration:
        raise Exception("All available ports already in use.")
