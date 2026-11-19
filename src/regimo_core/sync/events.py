"""Semantic events published by regimo-core on the databus.

Whenever the sync detects a change on the RDMO side (project created, updated
or removed), one event is published to ``edo.projects.v1`` with the shared
ontology-aware header contract (``regimo_core.interfaces.message``). The
sockets — notably socket-ux for the front-end project list — treat these
events accordingly; the payloads stay small (metadata only, no maDMP).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional

from regimo_core.interfaces.message import MessageHeader

#: Topic for project lifecycle events of the RDMO pull.
PROJECTS_TOPIC = "edo.projects.v1"

#: socket_id of this component in the message headers.
SOCKET_ID = "regimo-core"

#: schema_id carried in the message headers.
SCHEMA_ID = "edo.projects.v1"

EventKind = Literal["project.created", "project.updated", "project.removed"]


@dataclass
class SyncEvent:
    """One change on the RDMO side, ready for the bus and the log."""

    kind: EventKind
    project_id: int
    title: str
    project_iri: str = ""

    def payload(self) -> dict[str, Any]:
        return {
            "event": self.kind,
            "project": {
                "id": self.project_id,
                "title": self.title,
                "project_iri": self.project_iri,
            },
        }

    def header(self, ontology_uri: str = "") -> MessageHeader:
        lineage = f"rdmo#{self.project_id} -> madmp-exporter -> agnostic-trip -> fuseki"
        return MessageHeader.from_time(
            socket_id=SOCKET_ID,
            project_id=str(self.project_id),
            schema_id=SCHEMA_ID,
            ontology_uri=ontology_uri,
            lineage=lineage,
        )


def publish_event(client: Any, event: SyncEvent, ontology_uri: str = "") -> bool:
    """Best-effort publish via a ``BusClient``-like object; never raises."""
    if client is None:
        return False
    try:
        client.publish(PROJECTS_TOPIC, event.payload(), event.header(ontology_uri))
        return True
    except Exception as exc:  # bus problems must not break the sync
        import logging
        logging.getLogger(__name__).warning(
            "bus publish failed for %s: %s", event.kind, exc)
        return False


class LoggingPublisher:
    """Fallback publisher used when no Kafka broker is configured."""

    def publish(self, topic: str, payload: dict, header: MessageHeader) -> None:
        import logging
        logging.getLogger(__name__).info(
            "bus event (no producer): %s %s %s", topic, header.to_dict(), payload)


def optional_bus_publisher(bootstrap_servers: str) -> Optional[Any]:
    """A ``BusClient`` when a broker is configured, else a logging stub.

    The broker is contacted lazily by confluent-kafka; an unreachable broker
    surfaces as a warning at publish time (see :func:`publish_event`), not as
    a crash of the sync loop.
    """
    if not bootstrap_servers:
        return LoggingPublisher()
    from regimo_core.lib.bus import BusClient
    return BusClient(bootstrap_servers=bootstrap_servers, group_id=SOCKET_ID)
