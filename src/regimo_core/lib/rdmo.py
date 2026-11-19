"""HTTP client for the RDMO instance (CF1).

regimo-core pulls the project list from the RDMO REST API and each project's
maDMP from the export-plugin endpoint deployed in the instance
(``rdmo_madmp_exporter.api``: ``GET /api/v1/madmp/projects/{pk}/``). Both use
DRF token authentication — no RDMO/Django imports needed here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_RDMO_URL = "http://localhost:8000"
#: RDMO's ProjectViewSet paginates with a page size well below the project
#: count of a real instance, so the client follows ``next`` links.
_PAGE_SIZE = 100


class RDMOError(RuntimeError):
    """Raised when the RDMO instance cannot be reached or answers invalidly."""


@dataclass
class RDMOProject:
    """One project as listed by the RDMO API (change-detection metadata)."""

    id: int
    title: str
    description: str
    created: str
    updated: str
    last_changed: str

    @classmethod
    def from_api(cls, obj: dict[str, Any]) -> "RDMOProject":
        return cls(
            id=int(obj["id"]),
            title=str(obj.get("title") or ""),
            description=str(obj.get("description") or ""),
            created=str(obj.get("created") or ""),
            updated=str(obj.get("updated") or ""),
            last_changed=str(obj.get("last_changed") or ""),
        )


class RDMOClient:
    """Token-authenticated client for the RDMO REST API + maDMP endpoint."""

    def __init__(self, base_url: str = DEFAULT_RDMO_URL, token: str = "",
                 timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    # -- internals ---------------------------------------------------------------

    def _client(self) -> httpx.Client:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Token {self.token}"
        return httpx.Client(base_url=self.base_url, headers=headers,
                            timeout=self.timeout, follow_redirects=True)

    @staticmethod
    def _raise(response: httpx.Response, what: str) -> None:
        raise RDMOError(
            f"{what} failed: HTTP {response.status_code}: {response.text[:200]}")

    # -- API ---------------------------------------------------------------------

    def list_projects(self) -> list[RDMOProject]:
        """All projects on the RDMO side, following the API pagination."""
        projects: list[RDMOProject] = []
        url: str | None = (
            f"/api/v1/projects/projects/?page_size={_PAGE_SIZE}")
        with self._client() as client:
            while url is not None:
                response = client.get(url)
                if response.status_code != 200:
                    self._raise(response, "listing RDMO projects")
                data = response.json()
                rows = data.get("results", data) if isinstance(data, dict) else data
                projects.extend(RDMOProject.from_api(row) for row in rows)
                url = (data.get("next") or None) if isinstance(data, dict) else None
        return projects

    def get_madmp(self, project_id: int) -> dict[str, Any]:
        """The project's maDMP document, rendered by the export plugin."""
        with self._client() as client:
            response = client.get(f"/api/v1/madmp/projects/{project_id}/")
        if response.status_code == 403:
            raise RDMOError(
                f"maDMP export of project {project_id} not permitted for this token")
        if response.status_code == 404:
            raise RDMOError(f"RDMO project {project_id} not found")
        if response.status_code != 200:
            self._raise(response, f"maDMP export of project {project_id}")
        document = response.json()
        if not isinstance(document, dict) or "dmp" not in document:
            raise RDMOError(f"maDMP export of project {project_id} is not a "
                            f"{{'dmp': ...}} document")
        return document
