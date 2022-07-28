from os import environ

from dagster._core.events.log import EventLogEntry

MAXIMUM_EVENT_MESSAGE_CHARACTERS = environ.get(
    "DAGSTER_CLOUD_MAXIMUM_EVENT_MESSAGE_CHARACTERS", 50000
)


def truncate_event(
    event: EventLogEntry, maximum_length=MAXIMUM_EVENT_MESSAGE_CHARACTERS
) -> EventLogEntry:

    if len(event.user_message) > maximum_length:
        return event._replace(
            user_message=f"[TRUNCATED from {len(event.user_message)} characters to {MAXIMUM_EVENT_MESSAGE_CHARACTERS}] {event.user_message[:maximum_length]} [TRUNCATED]",
        )

    return event
