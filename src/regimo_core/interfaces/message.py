"""Ontology-aware Kafka message header / metadata contract.

Every message published by a Regimo socket carries this standardised set of
headers so downstream consumers can interpret, trace and lineage-check the
payload without guessing at its provenance or schema.

Headers (all UTF-8 strings):

``socket_id``
    Identifier of the producing socket, e.g. ``socket-rdmo``.
``ontology_uri``
    URI of the ontology / schema the payload conforms to ('' when unknown).
``project_id``
    Business identifier the payload belongs to (e.g. the source project).
``schema_id``
    Identifier of the schema/version the payload conforms to, e.g.
    ``madmp-lightmodel`` or ``https://doi.org/10.5281/zenodo.XXXXX``.
``timestamp``
    ISO-8601 UTC timestamp of publication (RFC 3339), used for sync.
``lineage``
    Provenance chain of the payload (e.g. ``rdmo#4 -> madmp-exporter``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Ordered header keys. Must stay in sync with :data:`ORDERED_HEADER_KEYS` used
# by the bus serialiser so every consumer sees the same ordering.
ORDERED_HEADER_KEYS: List[str] = [
    "socket_id",
    "ontology_uri",
    "project_id",
    "schema_id",
    "timestamp",
    "lineage",
]


@dataclass
class MessageHeader:
    """Standardised metadata attached to a bus message.

    The dataclass mirrors the Kafka ``Message.headers`` convention: each header
    is a ``(key, value)`` pair where ``value`` is ``bytes``. We keep the values
    as str here for ergonomics and let :meth:`to_headers` encode them.
    """

    socket_id: str
    project_id: str
    schema_id: str
    ontology_uri: str = ""
    timestamp: str = field(default_factory=lambda: _utc_now().isoformat())
    lineage: Optional[str] = None

    # --- construction helpers -------------------------------------------------

    @classmethod
    def from_time(
        cls,
        socket_id: str,
        project_id: str,
        schema_id: str,
        ontology_uri: str = "",
        lineage: Optional[str] = None,
    ) -> "MessageHeader":
        """Build a header snapshotted with the current UTC-time timestamp."""
        return cls(
            socket_id=socket_id,
            project_id=project_id,
            schema_id=schema_id,
            ontology_uri=ontology_uri,
            timestamp=_utc_now().isoformat(),
            lineage=lineage,
        )

    @classmethod
    def from_mapping(cls, mapping: Dict[str, Any]) -> "MessageHeader":
        """Build a header from a plain dict, tolerating absent keys."""
        return cls(
            socket_id=str(mapping.get("socket_id") or ""),
            ontology_uri=str(mapping.get("ontology_uri") or ""),
            project_id=str(mapping.get("project_id") or ""),
            schema_id=str(mapping.get("schema_id") or ""),
            timestamp=str(mapping.get("timestamp") or _utc_now().isoformat()),
            lineage=mapping.get("lineage"),
        )

    # --- (de)serialisation ----------------------------------------------------

    def to_headers(self) -> List[tuple]:
        """Encode into the Kafka ``Message.headers`` shape (UTF-8 bytes)."""
        return [
            (key, str(self.__dict__.get(key) or "").encode("utf-8"))
            for key in ORDERED_HEADER_KEYS
        ]

    @classmethod
    def from_headers(cls, headers: Optional[List[tuple]]) -> "MessageHeader":
        """Decode from the Kafka ``Message.headers`` shape (bytes values)."""
        raw: Dict[str, Any] = {}
        for key, value in headers or []:
            if isinstance(value, (bytes, bytearray)):
                raw[key] = value.decode("utf-8")
            else:
                raw[key] = value
        return cls.from_mapping(raw)

    def to_dict(self) -> Dict[str, str]:
        return {key: str(self.__dict__.get(key) or "") for key in ORDERED_HEADER_KEYS}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
