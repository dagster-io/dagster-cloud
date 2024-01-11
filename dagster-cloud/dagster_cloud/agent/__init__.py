from typing import List, NamedTuple, Optional

from dagster._serdes import (
    whitelist_for_serdes,
)

AgentQueue = Optional[str]


@whitelist_for_serdes
class AgentQueuesConfig(NamedTuple):
    include_default_queue: bool = True
    additional_queues: Optional[List[AgentQueue]] = None

    @staticmethod
    def default_queues() -> List[AgentQueue]:
        return [None]

    @property
    def queues(self) -> List[AgentQueue]:
        if self.additional_queues:
            queues = self.additional_queues.copy()
        else:
            queues = []
        if self.include_default_queue:
            for queue in AgentQueuesConfig.default_queues():
                queues.append(queue)
        return queues

    def matches(self, queue: AgentQueue):
        return queue in self.queues
