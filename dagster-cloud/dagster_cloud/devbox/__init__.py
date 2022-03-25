from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class DevBoxMetadata:
    deployment: str
    image: str
    authorized_keys: List[str]
    user_token: str


class DevBoxReconciler(ABC):
    @abstractmethod
    def describe_devboxes(self) -> Dict[str, DevBoxMetadata]:
        """Returns List[DevBoxMetadata] for all currently running devboxes"""
        ...

    @abstractmethod
    def start_devbox(self, devbox: DevBoxMetadata) -> None:
        """Start a new devbox"""
        ...

    @abstractmethod
    def stop_devbox(self, devbox: DevBoxMetadata) -> None:
        """Stop an existing devbox"""
        ...

    def reconcile(self, devboxes: Optional[List[DevBoxMetadata]] = None) -> None:
        requested_devboxes = {devbox.deployment: devbox for devbox in devboxes or []}
        existing_devboxes = self.describe_devboxes()

        # To remove
        for key in existing_devboxes.keys() - requested_devboxes.keys():
            self.stop_devbox(existing_devboxes[key])

        # To add
        for key in requested_devboxes.keys() - existing_devboxes.keys():
            self.start_devbox(requested_devboxes[key])

        # To redeploy
        for key in set(requested_devboxes.keys()).intersection(set(existing_devboxes.keys())):
            if existing_devboxes[key] != requested_devboxes[key]:
                self.stop_devbox(existing_devboxes[key])
                self.start_devbox(requested_devboxes[key])
