# Changelog

All notable changes to `regimo-core` are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/); versioning: SemVer.

## [0.2.0] - 2026-11-23

### Added
- `sync.round` heartbeat published on the `edo.sync.v1` topic after every sync round (including idle ones): created/updated/removed/skipped counts, skip reasons and duration — the basis for the UI's live sync/broadcast display (CF1-012).

### Fixed
- `regimo-core -h` printed an error instead of help (missing `context_settings`).

## [0.1.0] - 2026-11-23

First release — the CF1 sync engine.

### Added
- Kafka bus abstractions (client, ordered header contract `socket_id`/`ontology_uri`/`project_id`/`schema_id`/`timestamp`/`lineage`) with unit tests.
- Sync engine (`regimo-core sync`): lists RDMO projects (REST, token auth), pulls each project's maDMP from the export-plugin endpoint, digests content + curated SMS extras (SHA-256), transforms into the internal format and writes one Turtle artifact per project, replaces the Fuseki default graph from exactly the current artifacts, and publishes one semantic event per change on `edo.projects.v1`.
- Digest-stable state store: unchanged projects cost one project-list call + one maDMP call per project; vanished projects are removed together with their artifacts.
- maDMP → internal-format transform delegating to `fuseki-proto-tool[agnostic]` (linkml-map driven).
