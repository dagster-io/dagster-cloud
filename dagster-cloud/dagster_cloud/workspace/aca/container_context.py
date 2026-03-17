from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class AcaContainerContext:
    """Container context for Azure Container Apps code servers and run workers."""

    env_vars: list[str] = field(default_factory=list)
    server_resources: dict[str, Any] = field(default_factory=dict)
    run_resources: dict[str, Any] = field(default_factory=dict)

    def merge(self, other: AcaContainerContext) -> AcaContainerContext:
        """Merge another context on top of this one. Values in other take precedence."""
        return AcaContainerContext(
            env_vars=self.env_vars + other.env_vars,
            server_resources={**self.server_resources, **other.server_resources},
            run_resources={**self.run_resources, **other.run_resources},
        )

    @classmethod
    def create_from_config(cls, container_context: dict[str, Any] | None) -> AcaContainerContext:
        """Parse an AcaContainerContext from metadata.container_context."""
        aca_ctx = (container_context or {}).get("aca") or {}
        return cls(
            env_vars=list(aca_ctx.get("env_vars") or []),
            server_resources=dict(aca_ctx.get("server_resources") or {}),
            run_resources=dict(aca_ctx.get("run_resources") or {}),
        )

    def get_environment_dict(self) -> dict[str, str]:
        """Resolve env_vars list to a {KEY: VALUE} dict.

        Bare keys (no '=') are resolved from the current process environment.
        Missing bare keys are forwarded as empty strings so dg.EnvVar() can
        resolve them at plan time on the code server.
        """
        result: dict[str, str] = {}
        for entry in self.env_vars:
            if "=" in entry:
                k, v = entry.split("=", 1)
                result[k.strip()] = v
            else:
                key = entry.strip()
                if key:
                    result[key] = os.environ.get(key, "")
        return result
