"""JSON Kafka bus client on top of ``confluent-kafka``.

Provides a small, header-aware client used by every ``socket-*`` component. It
enforces the ontology-aware header contract from
:mod:`regimo_core.interfaces.message` and (de)serialises payloads as JSON.

For a development (no-docker) round trip you can point this client at the bus
provided by ``docker compose up zookeeper kafka`` (``localhost:9092``).
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from regimo_core.interfaces.message import MessageHeader

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TIMEOUT_S = 5.0


class BusClient:
    """Header-aware JSON Kafka producer and/or consumer.

    ``bootstrap_servers`` is the broker address the client connects to (from
    the host this is ``localhost:9092``). ``group_id`` is only relevant when
    consuming.
    """

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BROKER,
        group_id: str = "regimo-core",
        timeout_s: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.timeout_s = timeout_s

        conf = {"bootstrap.servers": bootstrap_servers}
        self._producer = Producer(conf)
        self._consumer: Optional[Consumer] = None

    # -- producer -------------------------------------------------------------

    def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        header: MessageHeader,
        flush: bool = True,
    ) -> None:
        """Publish a JSON payload with the given header to ``topic``.

        The value is JSON-serialised and the header attached via
        ``MessageHeader.to_headers()``.
        """
        value = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = header.to_headers()
        try:
            self._producer.produce(topic, value=value, headers=headers)
        except KafkaException as exc:
            self._producer.poll(0)
            raise BusError(str(exc)) from exc

        if flush:
            # nonzero timeout: surface delivery success/failure via the callback
            remainder = self._producer.flush(self.timeout_s)
            if remainder > 0:
                raise BusError("producer flush timed out; messages unsent")

    # -- consumer -------------------------------------------------------------

    def subscribe(self, topics: List[str], **kwargs: Any) -> None:
        """Subscribe to ``topics``; call :meth:`poll` to receive messages."""
        if self._consumer is None:
            self._consumer = Consumer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "group.id": self.group_id,
                    "auto.offset.reset": kwargs.get("auto_offset_reset", "earliest"),
                }
            )
        self._consumer.subscribe(topics)

    def poll(self) -> Optional[Dict[str, Any]]:
        """Poll for the next message; returns ``(payload, header)`` or ``None``.

        Blocks up to ``self.timeout_s``. Deserialises the JSON value and the
        header contract.
        """
        if self._consumer is None:
            raise BusError("not subscribed; call subscribe() first")

        msg = self._consumer.poll(self.timeout_s)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise BusError(msg.error().str())

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, AttributeError) as exc:
            raise BusError(f"non-JSON payload on {msg.topic()}: {exc}") from exc

        return {
            "payload": payload,
            "header": MessageHeader.from_headers(msg.headers()),
            "topic": msg.topic(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "offset": msg.offset(),
            "timestamp_ms": msg.timestamp(),
        }

    # -- helpers --------------------------------------------------------------

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()

    def publish_maDMP(self, path: str, header: MessageHeader, topic: str = "edo.dmp.v1") -> None:
        """Convenience: publish a maDMP ``{"dmp": ...}`` JSON file to ``topic``.

        Loads the file verbatim (it should contain a top-level ``{"dmp": ...}``
        object) and publishes it with the given header. Used for the
        first-payload round trip in CF1-001.
        """
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        self.publish(topic, payload, header)


class BusError(RuntimeError):
    """Raised for producer/consumer/connection failures in :class:`BusClient`."""
