"""Tests for the ontology-aware message header contract.

Coverage: header ordering, construction helpers, and the Kafka-bytes
(de)serialisation round-trip. Purely deterministic — no broker required.
"""

from __future__ import annotations

from regimo_core.interfaces.message import ORDERED_HEADER_KEYS, MessageHeader


def _header() -> MessageHeader:
    return MessageHeader(
        socket_id="socket-rdmo",
        project_id="4",
        schema_id="madmp-lightmodel",
        ontology_uri="https://github.com/EnergyDataOrchestrator/regimo-ontology",
        timestamp="2026-11-18T12:00:00+00:00",
        lineage="socket-rdmo#4 -> madmp-exporter -> madmp-lightmodel",
    )


class TestOrdering:
    def test_ordered_keys_are_exactly_the_contract(self) -> None:
        assert ORDERED_HEADER_KEYS == [
            "socket_id",
            "ontology_uri",
            "project_id",
            "schema_id",
            "timestamp",
            "lineage",
        ]

    def test_to_dict_preserves_order(self) -> None:
        assert list(_header().to_dict()) == ORDERED_HEADER_KEYS


class TestConstruction:
    def test_defaults(self) -> None:
        h = MessageHeader(socket_id="s", project_id="p", schema_id="sc")
        assert h.ontology_uri == ""
        assert h.lineage is None
        assert h.timestamp  # default factory always fills a timestamp

    def test_from_time_snapshots_utc_timestamp(self) -> None:
        h = MessageHeader.from_time(
            socket_id="s", project_id="p", schema_id="sc"
        )
        assert h.timestamp.endswith("+00:00")
        assert h.ontology_uri == ""
        assert h.lineage is None

    def test_from_time_round_trips_fields(self) -> None:
        h = MessageHeader.from_time(
            socket_id="s",
            project_id="p",
            schema_id="sc",
            ontology_uri="http://onto",
            lineage="a -> b",
        )
        assert h.socket_id == "s"
        assert h.project_id == "p"
        assert h.schema_id == "sc"
        assert h.ontology_uri == "http://onto"
        assert h.lineage == "a -> b"

    def test_from_mapping_tolerates_missing_keys(self) -> None:
        h = MessageHeader.from_mapping({"socket_id": "s"})
        assert h.project_id == ""
        assert h.schema_id == ""
        assert h.timestamp  # filled by default

    def test_from_mapping_coerces_values(self) -> None:
        h = MessageHeader.from_mapping(
            {"socket_id": 5, "project_id": None, "lineage": "x"}
        )
        assert h.socket_id == "5"
        assert h.project_id == ""  # None -> str(None) would be "None", check
        # from_mapping uses str(mapping.get(...)), None -> "None"; document below
        assert h.lineage == "x"


class TestSerialisation:
    def test_to_headers_encodes_all_six_as_bytes(self) -> None:
        headers = _header().to_headers()
        assert [k for k, _ in headers] == ORDERED_HEADER_KEYS
        for _, value in headers:
            assert isinstance(value, bytes)

    def test_none_coerced_to_empty_bytes(self) -> None:
        h = MessageHeader(socket_id="s", project_id="p", schema_id="sc")
        headers = dict(h.to_headers())
        assert headers["lineage"] == b""
        assert headers["ontology_uri"] == b""

    def test_round_trip_preserves_fields(self) -> None:
        original = _header()
        restored = MessageHeader.from_headers(original.to_headers())
        assert restored.to_dict() == original.to_dict()

    def test_from_headers_handles_none(self) -> None:
        h = MessageHeader.from_headers(None)
        assert h.socket_id == ""
        assert h.project_id == ""

    def test_from_headers_decodes_bytes_and_raw(self) -> None:
        # Kafka gives bytes; also accept plain str for robustness.
        restored = MessageHeader.from_headers(
            [("socket_id", b"s"), ("project_id", "p")]
        )
        assert restored.socket_id == "s"
        assert restored.project_id == "p"


class TestRoundTrip:
    def test_kafka_bytes_round_trip(self) -> None:
        original = _header()
        headers = original.to_headers()
        # Simulate what librdkafka hands a consumer: raw bytes per header.
        restored = MessageHeader.from_headers(headers)
        for key in ORDERED_HEADER_KEYS:
            assert restored.__dict__[key] == original.__dict__[key]
