# regimo-core
The foundational library and base abstractions for the Regimo Energy Data Orchestrator. Provides the internal engine for ontology-aware Kafka communication.

## Overview
The `regimo-core` repository contains the fundamental abstractions and internal libraries for **Regimo**, the microservice-based Energy Data Orchestrator developed at the **Karlsruhe Institute of Technology (KIT)**. It provides the "scaffolding" required for any functional socket to participate in the orchestrator's event-driven architecture.

## Core Responsibilities
As the engine of the ecosystem (referencing **KITopen-ID: 1000168804**), this repository manages:

* **Bus Orchestration:** Shared logic for Kafka producer/consumer patterns, ensuring consistent message delivery and robust error handling.
* **Semantic Middleware:** Internal tools for parsing the `regimo-ontology` and mapping it to runtime objects.
* **Socket Templates:** Base classes and interfaces that define how an "ontology-aware socket" should behave, significantly reducing boilerplate for new microservices.
* **Telemetry & Logging:** Standardised monitoring for energy data streams to ensure system health and data lineage.



## Repository Contents
* `lib/`: The core Python/Java libraries used as dependencies by all Regimo sockets.
* `interfaces/`: Definitions for the Kafka message headers and metadata required for ontology awareness.
* `utils/`: Common utilities for timestamp synchronisation, unit conversion (e.g., $kW$ to $MW$), and data validation.

## Usage
`regimo-core` is the primary dependency for all `socket-*` repositories. It is designed to be imported as a library or utilised as a base Docker image to ensure that every component in the Energy Data Orchestrator follows the same communication protocols.

---

## Modus Operandi
How this repository is developed, tested and released.

### Development environment
This is a **src-layout** package; the importable code lives under `src/regimo_core/`.
It is managed with **uv** and is installed into existing projects (e.g. the `edo`
umbrella) as a path dependency:

```toml
# edo/pyproject.toml (umbrella)
[tool.uv.sources]
regimo-core = { path = "core" }
```

The only runtime dependency is `confluent-kafka` (the broker client used by
`lib/bus.py`). Python >= 3.12 is required.

```sh
# install (runtime deps + the `dev` extra with pytest)
uv sync --extra dev
```

### Running the tests
```sh
# from the core/ directory, using the local venv
.venv/bin/python -m pytest
```

The suite is deliberately split into two tiers so that **most tests never need
a broker**:

* **Deterministic unit tests** (the default run) stub the underlying producer
  and cover the header contract (`interfaces/message.py`), JSON (de)serialisation
  and `BusClient` behaviour without touching docker. They are fast and safe to
  run anywhere.
* **`live` round-trip tests** are marked `@pytest.mark.live`. They hit a real
  broker on `localhost:9092`, publishing a maDMP payload to `edo.dmp.v1` and
  consuming it back with the full header contract intact. They **auto-skip**
  when no broker is reachable, so the default `pytest` run stays green on a
  machine without docker:

```sh
# bring the bus up first, if you want the live tests to actually run
docker compose up -d zookeeper kafka

# force only the live tests
.venv/bin/python -m pytest -m live
```

### The CF1-001 round-trip
`components/utils/lib/kafka/roundtrip_madmp.py` (in the `edo` umbrella, not here)
is the canonical end-to-end check that the maDMP first payload survives the bus
with its ontology-aware headers intact. It is the same logic the live test
exercises programmatically. See the [[CF1-001]] card for the full activity log.

### Header contract invariants
Keep `ORDERED_HEADER_KEYS` in `interfaces/message.py` the single source of
truth for the six headers (`socket_id`, `ontology_uri`, `project_id`,
`schema_id`, `timestamp`, `lineage`). The bus serialiser and the tests all
derive from it; never add a header without updating the contract, the
serialiser and the tests together.

### Versioning / releases
Bumps bump `__version__` in `src/regimo_core/__init__.py`, the `version` field in
`pyproject.toml`, and the consuming umbrella's `uv.lock`/path source in lockstep.