"""The RDMO -> internal-database sync engine (CF1).

One ``run_once`` does:

1. list the projects on the RDMO instance (REST API, token auth),
2. pull each project's maDMP from the export-plugin endpoint and digest it
   (SHA-256 over the canonical JSON),
3. transform new/changed maDMPs into the internal format (canonical
   ``fuseki-proto`` agnostic-trip, DMP half), skolemize blank nodes into
   stable project IRIs (REV-001 #1) and write one Turtle artifact per
   project,
4. drop artifacts of projects that vanished on the RDMO side,
5. update the Fuseki default graph: per-project upsert/removal when every
   artifact is skolemized, whole-graph replace otherwise (fallback),
6. publish one semantic event per change on the ``edo.projects.v1`` topic.

Unchanged projects are skipped entirely (digest comparison), so a steady
state run only costs one project-list call + one maDMP call per project.
A per-project artifact whose file is missing or whose content digest no
longer matches the persisted state is re-transformed (self-heal after a
crashed or failed store update, or after a rollout that changes the
artifact format).
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from regimo_core.lib.fuseki import FusekiStore
from regimo_core.lib.rdmo import RDMOClient, RDMOProject
from regimo_core.sync.events import SyncEvent, publish_event, publish_round_event
from regimo_core.sync.skolemize import SkolemizationError, skolemize
from regimo_core.sync.state import ProjectState, SyncState

logger = logging.getLogger(__name__)

#: Callable turning ``{"dmp": ...}`` into ``(turtle, meta)``; provided by
#: :mod:`regimo_core.sync.transform` (wrapping fuseki-proto).
TransformFn = Callable[[dict], "tuple[str, dict]"]


def madmp_digest(document: dict[str, Any], extras_bytes: bytes = b"") -> str:
    """Stable SHA-256 over the maDMP content plus the curated extras.

    The extras (SMS device/property records, finding F12; the columns
    manifest, UPL-004 Stage 2 decision A) end up in the same graph, so they
    must participate in change detection.
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
    # how the store was updated this round: "upsert" (per-project closures),
    # "replace" (whole default graph) or "" (no store change needed)
    store_mode: str = ""
    # project_iri of each removed project, captured before the state entry
    # vanished — needed for the per-project closure delete
    removed_iris: dict[int, str] = field(default_factory=dict)

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
        columns_extras_dir: Optional[Path] = None,
    ) -> None:
        self.client = client
        self.store = store
        self.state = state
        self.artifacts_dir = Path(artifacts_dir)
        self.transform = transform if transform is not None else _default_transform
        self.bus = bus
        self.ontology_uri = ontology_uri
        self.sms_extras_dir = Path(sms_extras_dir) if sms_extras_dir else None
        self.columns_extras_dir = Path(columns_extras_dir) if columns_extras_dir else None

    # -- main loop ------------------------------------------------------------

    def run_once(self) -> SyncResult:
        started = time.monotonic()
        result = SyncResult()
        projects = self.client.list_projects()
        remote_ids = {p.id for p in projects}
        skolemization_failed = False

        for project in projects:
            if self._sync_project(project, result):
                skolemization_failed = True

        for project_id in sorted(self.state.known_ids() - remote_ids):
            self._forget(project_id, result)

        if result.changed or self.state.full_replace_pending:
            self._update_store(result, force_replace=skolemization_failed)

        for event in result.events:
            publish_event(self.bus, event, self.ontology_uri)

        # one diagnostic event per round (heartbeat): the UI shows that the
        # sync is alive and what it skipped, not only what it changed
        publish_round_event(
            self.bus, result,
            duration_ms=int((time.monotonic() - started) * 1000),
            ontology_uri=self.ontology_uri,
            plans=self._plans(),
        )

        self.state.save()
        return result

    # -- one project ----------------------------------------------------------

    def _sync_project(self, project: RDMOProject, result: SyncResult) -> bool:
        """Sync one project; returns True when skolemization failed (the
        whole round then falls back to the graph-replace path)."""
        known = self.state.get(project.id)
        try:
            document = self.client.get_madmp(project.id)
        except Exception as exc:
            result.skipped.append((project.id, f"maDMP export failed: {exc}"))
            logger.warning("project %s (%s): skip — %s", project.id, project.title, exc)
            return False

        device, properties, extras_paths = self._sms_extras(project.id)
        columns, columns_paths = self._columns_extras(project.id)
        extras_bytes = b"".join(path.read_bytes() for path in extras_paths)
        extras_bytes += b"".join(path.read_bytes() for path in columns_paths)
        digest = madmp_digest(document, extras_bytes)
        if known is not None and known.madmp_sha256 == digest:
            # Refresh cheap metadata so the state file stays truthful even
            # when only RDMO bookkeeping fields changed.
            known.title, known.last_changed, known.updated = (
                project.title, project.last_changed, project.updated)
            if self._artifact_is_intact(known):
                return False
            # Artifact missing or content drifted (failed upload, rollout,
            # manual edit) — fall through and re-transform from the maDMP.
            logger.info("project %s (%s): artifact drift — re-transforming",
                        project.id, project.title)

        try:
            # Older transform callbacks (test doubles) predate the columns
            # parameter; detect arity once instead of retrying on TypeError,
            # which would mask a transform's own internal TypeError.
            if "columns" in inspect.signature(self.transform).parameters:
                turtle, meta = self.transform(document, device, properties, columns)
            else:
                turtle, meta = self.transform(document, device, properties)
        except Exception as exc:
            result.skipped.append((project.id, f"transform failed: {exc}"))
            logger.warning("project %s (%s): skip — %s", project.id, project.title, exc)
            return False

        project_iri = str(meta.get("project_iri") or "")
        sensor_iri = str(meta.get("sensor_iri") or "")
        failed = False
        try:
            turtle = skolemize(turtle, project_iri)
        except SkolemizationError as exc:
            logger.warning("project %s: skolemization failed (%s) — "
                           "falling back to a whole-graph replace", project.id, exc)
            failed = True
        graph_sha256 = _sha256(turtle)
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
            sensor_iri=sensor_iri,
            graph_sha256=graph_sha256,
            artifact=artifact,
        ))
        event = SyncEvent(kind=kind, project_id=project.id,  # type: ignore[arg-type]
                          title=project.title, project_iri=project_iri,
                          plan_digest_sha256=digest, graph_sha256=graph_sha256)
        result.events.append(event)
        (result.updated if known is not None else result.created).append(project.id)
        logger.info("project %s (%s): %s", project.id, project.title, kind)
        return failed

    def _forget(self, project_id: int, result: SyncResult) -> None:
        state = self.state.remove(project_id)
        title = state.title if state else str(project_id)
        if state:
            result.removed_iris[project_id] = state.project_iri
            if state.artifact:
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

    # -- columns extras (UPL-004 Stage 2, decision A: the publisher owns the
    #    column structure; the manifest is written by the data-file side at
    #    publish time) ----------------------------------------------------------

    def _columns_extras(self, project_id: int) -> "tuple[Optional[list[dict]], list[Path]]":
        """Load the columns manifest of one RDMO project.

        Layout: ``<columns_extras_dir>/<project_id>/columns.json`` — a
        ColumnsManifest whose ``datasets`` entries are matched to the maDMP
        datasets inside the transform (rule C6). ``None`` when absent or
        malformed — the transform then runs without data-file columns.
        """
        if self.columns_extras_dir is None:
            return None, []
        manifest_path = self.columns_extras_dir / str(project_id) / "columns.json"
        if not manifest_path.is_file():
            return None, []
        manifest = _load_json(manifest_path)
        datasets = manifest.get("datasets") or []
        if not isinstance(datasets, list):
            logger.warning("project %s: columns manifest ignored — "
                           "'datasets' is not a list", project_id)
            return None, []
        logger.info("project %s: columns extras (datasets=%d)",
                    project_id, len(datasets))
        return datasets, [manifest_path]

    # -- artifacts ------------------------------------------------------------

    def _write_artifact(self, project_id: int, turtle: str) -> str:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        name = f"{project_id}.ttl"
        (self.artifacts_dir / name).write_text(turtle, encoding="utf-8")
        return name

    def _artifact_is_intact(self, state: ProjectState) -> bool:
        """True when the artifact file exists and still hashes to the digest
        recorded when it was pushed to Fuseki (drift detection)."""
        if not state.artifact or not state.graph_sha256:
            return False
        path = self.artifacts_dir / state.artifact
        if not path.exists():
            return False
        return _sha256(path.read_text(encoding="utf-8")) == state.graph_sha256

    # -- store update -----------------------------------------------------------

    def _update_store(self, result: SyncResult, force_replace: bool = False) -> None:
        turtles = self.state.artifacts(self.artifacts_dir)
        replace = (
            force_replace
            or self.state.full_replace_pending
            or any(not s.project_iri for s in self.state.projects.values())
        )
        if replace:
            self._replace_store(result, turtles)
            return
        try:
            for project_iri in result.removed_iris.values():
                if project_iri:
                    self.store.remove_project(project_iri)
            for project_id in sorted(set(result.created) | set(result.updated)):
                state = self.state.get(project_id)
                if state is None or not state.artifact or not state.project_iri:
                    continue
                turtle = (self.artifacts_dir / state.artifact).read_text(
                    encoding="utf-8")
                self.store.upsert_project(state.project_iri, state.sensor_iri,
                                          turtle)
            result.store_mode = "upsert"
        except Exception as exc:
            logger.warning("per-project upsert failed (%s) — falling back to "
                           "a whole-graph replace", exc)
            self.state.full_replace_pending = True
            self._replace_store(result, turtles)

    def _replace_store(self, result: SyncResult,
                       turtles: list[tuple[str, str]]) -> None:
        try:
            self.store.replace_all(turtles)
        except Exception:
            # Persist the retry flag *before* propagating so the next round
            # re-uploads everything even when no maDMP changed.
            self.state.full_replace_pending = True
            self.state.save()
            raise
        self.state.full_replace_pending = False
        result.graph_replaced = True
        result.store_mode = "replace"
        logger.info("replaced default graph with %d project artifact(s)",
                    len(turtles))

    def _plans(self) -> list[dict[str, str]]:
        """Per-project digests for the round heartbeat (ADP badge feed)."""
        return [
            {
                "project_id": str(state.project_id),
                "title": state.title,
                "project_iri": state.project_iri,
                "plan_digest_sha256": state.madmp_sha256,
                "graph_sha256": state.graph_sha256,
            }
            for state in sorted(self.state.projects.values(),
                                key=lambda s: (s.title, s.project_id))
        ]


def _default_transform(document: dict, device: dict | None = None,
                       properties: list[dict] | None = None,
                       columns: list[dict] | None = None) -> "tuple[str, dict]":
    # Imported lazily so importing regimo_core.sync never pulls fuseki-proto.
    from regimo_core.sync.transform import transform_madmp
    return transform_madmp(document, device, properties, columns)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))
