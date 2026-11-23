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

### The RDMO sync engine (CF1)
`src/regimo_core/sync/` is the pull-based sync between an RDMO instance and the
internal graph database (Fuseki). One `regimo-core sync` run (CLI in `cli.py`)
does:

1. list the projects on the RDMO instance (REST API, token auth — `lib/rdmo.py`),
2. pull each project's maDMP from the export-plugin endpoint
   (`api/v1/madmp/projects/{pk}/`, provided by `components/socket-rdmo/madmp-exporter`)
   and digest it (SHA-256 over the canonical JSON plus any curated SMS extras),
3. transform new/changed maDMPs into the internal format via the canonical
   `fuseki-proto` agnostic-trip transform (`sync/transform.py`), skolemize
   blank nodes into stable project-scoped IRIs (`sync/skolemize.py`, REV-001 #1)
   and write one Turtle artifact per project,
4. drop artifacts of projects that vanished on the RDMO side,
5. update the Fuseki default graph (`lib/fuseki.py`): because the artifacts
   are skolemized, one changed project is upserted in place (closure delete
   + re-upload) and a removed project's closure is deleted — a whole-graph
   replace remains the fallback for unskolemized content and after failures
   (persisted via `full_replace_pending`, self-heals on the next round),
6. publish one semantic event per change on `edo.projects.v1`
   (`sync/events.py`) — consumed by `socket-ux` and forwarded to the
   frontend as server-sent events.

Unchanged projects are skipped via the persisted digest (`sync/state.json`),
so a steady-state run costs one project-list call plus one maDMP call per
project. SMS devices/properties are *not* part of the RDMO pull (finding F12:
the dataset↔sensor link lives outside the maps); they are curated per project
under `data/regimo-core/sms/<project_id>/` (`device.json` + `property-*.json`)
and participate in the digest via `--sms-extras-dir`.

Two digests are persisted per project and exposed on the bus: the
**plan digest** (`madmp_sha256`, content of maDMP + SMS extras) and the
**graph digest** (`graph_sha256`, SHA-256 of the skolemized artifact pushed
to Fuseki). Change events carry both; the round heartbeat on `edo.sync.v1`
lists them for every known project (`plans`), and an artifact file whose
content no longer matches its graph digest is re-transformed automatically.

```sh
# from the edo umbrella (env in .env: RDMO_URL, RDMO_API_TOKEN, ...)
.venv/bin/regimo-core sync --once --kafka localhost:9092 -v
```

Projects whose maDMP cannot be transformed (e.g. no `project` array, no
`access_url` — genuine data gaps in the RDMO answers) are skipped with a
logged reason and do not block the rest of the sync.

### Header contract invariants
Keep `ORDERED_HEADER_KEYS` in `interfaces/message.py` the single source of
truth for the six headers (`socket_id`, `ontology_uri`, `project_id`,
`schema_id`, `timestamp`, `lineage`). The bus serialiser and the tests all
derive from it; never add a header without updating the contract, the
serialiser and the tests together.

### Versioning / releases
Bumps bump `__version__` in `src/regimo_core/__init__.py`, the `version` field in
`pyproject.toml`, and the consuming umbrella's `uv.lock`/path source in lockstep.