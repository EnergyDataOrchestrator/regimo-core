"""regimo-core: foundational library for ontology-aware Kafka communication.

The shared engine of the Regimo ecosystem. Provides the internal abstractions
used by every ``socket-*`` component to participate in the orchestrator's
event-driven architecture:

* :mod:`regimo_core.interfaces` — Kafka message header / metadata contract.
* :mod:`regimo_core.lib` — shared producer/consumer bus logic.
* :mod:`regimo_core.utils` — timestamp synchronisation, unit conversion.
"""

__version__ = "0.3.0"
