"""Tests for the JSON Kafka bus client.

Split into two tiers:

* deterministic unit tests that stub the underlying ``_producer`` so they need
  no broker (never require docker);
* a ``live``-marked round trip that runs against a real broker at
  ``localhost:9092`` when one is reachable and auto-skips otherwise
  (``docker compose up -d zookeeper kafka``). Run with
  ``pytest -m live`` to force them, or leave them in the default run where
  they self-skip.
"""

from __future__ import annotations

import json

import pytest

from regimo_core.interfaces.message import MessageHeader
from regimo_core.lib.bus import DEFAULT_BROKER, BusClient, BusError

TOPIC = "edo.dmp.v1"


def _header() -> MessageHeader:
    return MessageHeader.from_time(
        socket_id="socket-rdmo",
        project_id="4",
        schema_id="madmp-lightmodel",
        ontology_uri="https://github.com/EnergyDataOrchestrator/regimo-ontology",
        lineage="socket-rdmo#4 -> madmp-exporter -> madmp-lightmodel",
    )


class FakeProducer:
    """Stand-in for the (C-bound, non-mockable) confluent producer.

    ``regimo_core`` only uses ``produce()`` and ``flush()`` on the producer, so a
    small python fake is enough to exercise the bus logic without a broker.
    """

    def __init__(self, flush_remainder: int = 0) -> None:
        self.calls: list[dict] = []
        self.flush_remainder = flush_remainder
        self.poll_calls = 0

    def produce(self, topic: str, value=None, **kwargs) -> None:
        self.calls.append({"topic": topic, "value": value, **kwargs})

    def flush(self, timeout=0) -> int:
        return self.flush_remainder

    def poll(self, *args, **kwargs) -> None:
        self.poll_calls += 1

    @property
    def last(self) -> dict:
        return self.calls[-1]


@pytest.fixture
def client() -> BusClient:
    # broker address is never contacted by the stubbed unit tests below
    return BusClient(bootstrap_servers="unused:9092")


# --- publish (stubbed producer, no broker needed) ------------------------------


class TestPublishUnit:
    def test_publish_calls_produce_with_json_bytes(self, client) -> None:
        """The value handed to the producer is UTF-8 JSON and the ordered
        headers are attached as bytes."""
        fake = FakeProducer()
        client._producer = fake

        client.publish(TOPIC, {"dmp": {"title": "x"}}, _header())

        call = fake.last
        assert call["topic"] == TOPIC
        payload = json.loads(call["value"].decode("utf-8"))
        assert payload == {"dmp": {"title": "x"}}
        keys = [k for k, _ in call["headers"]]
        assert keys == [
            "socket_id",
            "ontology_uri",
            "project_id",
            "schema_id",
            "timestamp",
            "lineage",
        ]

    def test_publish_serialises_unicode(self, client) -> None:
        """JSON values must not be mangled ASCII ― non-Latin1 payloads survive."""
        fake = FakeProducer()
        client._producer = fake

        client.publish(TOPIC, {"dmp": {"titel": "Energiedaten — Messung ü"}}, _header())
        payload = json.loads(fake.last["value"].decode("utf-8"))
        assert payload["dmp"]["titel"] == "Energiedaten — Messung ü"

    def test_publish_raises_bus_error_when_flush_times_out(self, client) -> None:
        """A non-zero remainder from ``flush`` means messages are unsent."""
        fake = FakeProducer(flush_remainder=2)
        client._producer = fake

        with pytest.raises(BusError, match="flush timed out"):
            client.publish(TOPIC, {"dmp": {}}, _header())


class TestPublishMaDMP:
    def test_publishes_verbatim_file(self, client, tmp_path) -> None:
        """``publish_maDMP`` loads the JSON file and publishes it as-is."""
        fake = FakeProducer()
        client._producer = fake
        payload = {"dmp": {"project": "measurement_02"}}
        path = tmp_path / "dmp.json"
        path.write_text(json.dumps(payload), encoding="utf-8")

        client.publish_maDMP(str(path), _header(), topic=TOPIC)

        assert fake.last["topic"] == TOPIC
        assert json.loads(fake.last["value"].decode("utf-8")) == payload


# --- consumer sanity (no broker needed) ----------------------------------------


class TestConsumerUnit:
    def test_poll_before_subscribe_raises(self, client) -> None:
        with pytest.raises(BusError, match="not subscribed"):
            client.poll()


# --- live round trip (broker required, auto-skipped otherwise) ------------------


def _broker_reachable() -> bool:
    try:
        from confluent_kafka import Producer

        client = Producer({"bootstrap.servers": DEFAULT_BROKER})
        client.list_topics(timeout=5)
        return True
    except Exception:
        return False


@pytest.mark.live
def test_maDMP_round_trip_on_live_broker(tmp_path) -> None:
    """Publish a maDMP payload to ``edo.dmp.v1`` and consume it back intact.

    Uses a unique consumer group + topic suffix so the test is independent of
    whatever else is on the bus and reads only what it wrote.
    """
    import time
    import uuid

    if not _broker_reachable():
        pytest.skip("no Kafka broker reachable at %s" % DEFAULT_BROKER)

    topic = f"edo.dmp.v1.test.{uuid.uuid4().hex[:8]}"
    payload = {"dmp": {"title": "round trip", "project": "measurement_02"}}
    path = tmp_path / "dmp.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    producer = BusClient(bootstrap_servers=DEFAULT_BROKER)
    producer.publish_maDMP(str(path), _header(), topic=topic)

    consumer = BusClient(
        bootstrap_servers=DEFAULT_BROKER,
        group_id=f"cf1-001-test-{uuid.uuid4().hex[:8]}",
    )
    consumer.subscribe([topic], auto_offset_reset="earliest")
    time.sleep(1)

    msg = None
    try:
        for _ in range(20):
            msg = consumer.poll()
            if msg is not None and msg["payload"] == payload:
                break
            time.sleep(0.5)
    finally:
        consumer.close()

    assert msg is not None, "no matching message consumed"
    assert msg["payload"] == payload
    header_dict = msg["header"].to_dict()
    assert header_dict["socket_id"] == "socket-rdmo"
    assert header_dict["project_id"] == "4"
    assert header_dict["schema_id"] == "madmp-lightmodel"
