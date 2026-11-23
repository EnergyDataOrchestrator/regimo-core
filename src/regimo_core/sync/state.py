"""Durable state of the RDMO -> internal-database sync.

A small JSON file maps each known RDMO project to the maDMP content digest it
was last synced with and the Turtle artifact written for it. The artifacts
(``<artifacts-dir>/<project_id>.ttl``) are the source of truth for what the
engine pushes into Fuseki: on any change the default graph is replaced from
exactly these files, so removed projects disappear and stale triples cannot
accumulate.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ProjectState:
    """Everything regimo-core remembers about one synced RDMO project."""

    project_id: int
    title: str
    created: str = ""
    updated: str = ""
    last_changed: str = ""
    madmp_sha256: str = ""
    project_iri: str = ""
    sensor_iri: str = ""  # curated SMS device IRI (meta.sensor_iri), if any
    graph_sha256: str = ""  # SHA-256 of the artifact content pushed to Fuseki
    artifact: str = ""  # file name inside the artifacts dir


@dataclass
class SyncState:
    """``{project_id -> ProjectState}`` persisted as one JSON file."""

    path: Path
    projects: dict[int, ProjectState] = field(default_factory=dict)
    # Set after a failed store update so the next round re-uploads *all*
    # artifacts even when no maDMP changed (self-heal without digest churn).
    full_replace_pending: bool = False

    # -- persistence ---------------------------------------------------------

    def load(self) -> "SyncState":
        if self.path.exists():
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.projects = {
                int(pid): ProjectState(
                    project_id=int(pid),
                    **{k: str(v) for k, v in data.items() if k != "project_id"},
                )
                for pid, data in raw.get("projects", {}).items()
            }
            self.full_replace_pending = bool(raw.get("full_replace_pending", False))
        return self

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "full_replace_pending": self.full_replace_pending,
            "projects": {
                str(pid): {k: v for k, v in vars(state).items() if k != "project_id"}
                for pid, state in self.projects.items()
            }
        }
        self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=False),
                             encoding="utf-8")

    # -- accessors -----------------------------------------------------------

    def get(self, project_id: int) -> ProjectState | None:
        return self.projects.get(project_id)

    def put(self, state: ProjectState) -> None:
        self.projects[state.project_id] = state

    def remove(self, project_id: int) -> ProjectState | None:
        return self.projects.pop(project_id, None)

    def known_ids(self) -> set[int]:
        return set(self.projects)

    def artifacts(self, artifacts_dir: Path) -> list[tuple[str, str]]:
        """``[(file_name, turtle), ...]`` for every project artifact present."""
        result: list[tuple[str, str]] = []
        for state in sorted(self.projects.values(), key=lambda s: (s.title, s.project_id)):
            path = artifacts_dir / state.artifact if state.artifact else None
            if path and path.exists():
                result.append((path.name, path.read_text(encoding="utf-8")))
        return result
