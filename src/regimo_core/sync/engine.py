"""The RDMO -> internal-database sync engine (CF1).

One ``run_once`` does:

1. list the projects on the RDMO instance (REST API, token auth),
2. pull each project's maDMP from the export-plugin endpoint and digest it
   (SHA-256 over the canonical JSON),
3. transform new/changed maDMPs into the internal format (canonical
   ``fuseki-proto`` agnostic-trip, DMP half) and write one Turtle artifact
   per project,
4. drop artifacts of projects that vanished on the RDMO side,
5. on any change, replace the Fuseki default graph from exactly the current
   artifacts (blank nodes make per-project deletion unreliable),
6. publish one semantic event per change on the ``edo.projects.v1`` topic.

Unchanged projects are skipped entirely (digest comparison), so a steady
state run only costs one project-list call + one maDMP call per project.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from regimo_core.lib.fuseki import FusekiStore
from regimo_core.lib.rdmo import RDMOClient, RDMOProject
from regimo_core.sync.events import SyncEvent, publish_event
from regimo_core.sync.state import ProjectState, SyncState

logger = logging.getLogger(__name__)

#: Callable turning ``{"dmp": ...}`` into ``(turtle, meta)``; provided by
#: :mod:`regimo_core.sync.transform` (wrapping fuseki-proto).
TransformFn = Callable[[dict], "tuple[str, dict]"]


def madmp_digest(document: dict[str, Any], extras_bytes: bytes = b"") -> str:
    """Stable SHA-256 over the maDMP content plus the curated SMS extras.

    The extras (device/property records, finding F12) end up in the same
    graph, so they must participate in change detection.
    """
    canonical = json.dumps(document, sort_keys=True, ensure_ascii=False)
    hasher = hashlib.sha256()
    hasher.update(canonical.encode("utf-8"))
    hasher.update(extras_bytes)
    return hasher.hexdigest()


@dataclass
class SyncResult:
    """What one ``run_once`` did — returned for the CLI and tests."""

    created: list[int] = field(default_factory=list)
    updated: list[int] = field(default_factory=list)
    removed: list[int] = field(default_factory=list)
    skipped: list[tuple[int, str]] = field(default_factory=list)  # (id, reason)
    events: list[SyncEvent] = field(default_factory=list)
    graph_replaced: bool = False

    @property
    def changed(self) -> bool:
        return bool(self.created or self.updated or self.removed)


class SyncEngine:
    """Pulls RDMO, keeps the internal database in step, announces changes."""

    def __init__(
        self,
        client: RDMOClient,
        store: FusekiStore,
        state: SyncState,
        artifacts_dir: Path,
        transform: Optional[TransformFn] = None,
        bus: Optional[Any] = None,
        ontology_uri: str = "",
        sms_extras_dir: Optional[Path] = None,
    ) -> None:
        self.client = client
        self.store = store
        self.state = state
        self.artifacts_dir = Path(artifacts_dir)
        self.transform = transform if transform is not None else _default_transform
        self.bus = bus
        self.ontology_uri = ontology_uri
        self.sms_extras_dir = Path(sms_extras_dir) if sms_extras_dir else None

    # -- main loop ------------------------------------------------------------

    def run_once(self) -> SyncResult:
        result = SyncResult()
        projects = self.client.list_projects()
        remote_ids = {p.id for p in projects}

        for project in projects:
            self._sync_project(project, result)

        for project_id in sorted(self.state.known_ids() - remote_ids):
            self._forget(project_id, result)

        if result.changed:
            turtles = self.state.artifacts(self.artifacts_dir)
            self.store.replace_all(turtles)
            result.graph_replaced = True
            logger.info("replaced default graph with %d project artifact(s)",
                        len(turtles))

        for event in result.events:
            publish_event(self.bus, event, self.ontology_uri)

        self.state.save()
        return result

    # -- one project ----------------------------------------------------------

    def _sync_project(self, project: RDMOProject, result: SyncResult) -> None:
        known = self.state.get(project.id)
        try:
            document = self.client.get_madmp(project.id)
        except Exception as exc:
            result.skipped.append((project.id, f"maDMP export failed: {exc}"))
            logger.warning("project %s (%s): skip — %s", project.id, project.title, exc)
            return

        device, properties, extras_paths = self._sms_extras(project.id)
        extras_bytes = b"".join(path.read_bytes() for path in extras_paths)
        digest = madmp_digest(document, extras_bytes)
        if known is not None and known.madmp_sha256 == digest:
            # Refresh cheap metadata so the state file stays truthful even
            # when only RDMO bookkeeping fields changed.
            known.title, known.last_changed, known.updated = (
                project.title, project.last_changed, project.updated)
            return

        try:
            turtle, meta = self.transform(document, device, properties)
        except Exception as exc:
            result.skipped.append((project.id, f"transform failed: {exc}"))
            logger.warning("project %s (%s): skip — %s", project.id, project.title, exc)
            return

        project_iri = str(meta.get("project_iri") or "")
        artifact = self._write_artifact(project.id, turtle)
        kind = "project.updated" if known is not None else "project.created"
        self.state.put(ProjectState(
            project_id=project.id,
            title=project.title,
            created=project.created,
            updated=project.updated,
            last_changed=project.last_changed,
            madmp_sha256=digest,
            project_iri=project_iri,
            artifact=artifact,
        ))
        event = SyncEvent(kind=kind, project_id=project.id,  # type: ignore[arg-type]
                          title=project.title, project_iri=project_iri)
        result.events.append(event)
        (result.updated if known is not None else result.created).append(project.id)
        logger.info("project %s (%s): %s", project.id, project.title, kind)

    def _forget(self, project_id: int, result: SyncResult) -> None:
        state = self.state.remove(project_id)
        title = state.title if state else str(project_id)
        if state and state.artifact:
            artifact = self.artifacts_dir / state.artifact
            artifact.unlink(missing_ok=True)
        result.removed.append(project_id)
        result.events.append(SyncEvent(kind="project.removed",
                                       project_id=project_id, title=title,
                                       project_iri=state.project_iri if state else ""))
        logger.info("project %s (%s): removed (gone from RDMO)", project_id, title)

    # -- SMS extras (finding F12: the dataset<->sensor link lives outside the
    #    maps, so it is an explicitly curated per-project directory) -------------

    def _sms_extras_paths(self, project_id: int) -> "list[Path | None]":
        """Curated record files for one RDMO project: device.json first."""
        if self.sms_extras_dir is None:
            return []
        project_dir = self.sms_extras_dir / str(project_id)
        device_path = project_dir / "device.json"
        paths: list[Path | None] = [device_path if device_path.exists() else None]
        paths.extend(sorted(project_dir.glob("property-*.json")))
        return paths

    def _sms_extras(self, project_id: int
                    ) -> "tuple[dict | None, list[dict], list[Path]]":
        """Load the curated SMS records of one RDMO project.

        Returns ``(device, properties, record_paths)``; ``record_paths`` feeds
        the change digest, the records feed the transform. Layout:
        ``<sms_extras_dir>/<project_id>/device.json`` plus
        ``property-*.json``; empty when absent — the transform then runs
        DMP-only.
        """
        paths = self._sms_extras_paths(project_id)
        device = _load_json(paths[0]) if paths and paths[0] else None
        properties = [_load_json(p) for p in paths[1:]]
        if device is None and not properties:
            return None, [], []
        logger.info("project %s: SMS extras (device=%s, properties=%d)",
                    project_id, bool(device), len(properties))
        return device, properties, [p for p in paths if p]

    # -- artifacts ------------------------------------------------------------

    def _write_artifact(self, project_id: int, turtle: str) -> str:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        name = f"{project_id}.ttl"
        (self.artifacts_dir / name).write_text(turtle, encoding="utf-8")
        return name


def _default_transform(document: dict, device: dict | None = None,
                       properties: list[dict] | None = None) -> "tuple[str, dict]":
    # Imported lazily so importing regimo_core.sync never pulls fuseki-proto.
    from regimo_core.sync.transform import transform_madmp
    return transform_madmp(document, device, properties)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))
