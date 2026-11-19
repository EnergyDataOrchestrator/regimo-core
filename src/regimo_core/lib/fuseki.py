"""Internal graph database (Fuseki ``/projects``) storage for regimo-core.

Mirrors the HTTP usage of ``fuseki-proto``'s ``docker.py``: Turtle upload to
``/{dataset}/data`` (default graph) and ``CLEAR DEFAULT`` via the SPARQL
update endpoint. The sync engine keeps one Turtle artifact per project and
replaces the whole default graph on every change — with blank nodes in the
internal format this is the only reliable way to *update* a single project
(``DELETE`` of one project's bnode closure is not expressible cleanly).
"""

from __future__ import annotations

from typing import Any

import httpx

DEFAULT_FUSEKI_URL = "http://localhost:3031"
DEFAULT_DATASET = "projects"
DEFAULT_USER = "admin"
DEFAULT_PASSWORD = "edo-proto"


class FusekiError(RuntimeError):
    """Raised when the internal database cannot be reached or refuses data."""


class FusekiStore:
    """Basic-auth client for the default graph of one Fuseki dataset."""

    def __init__(self, base_url: str = DEFAULT_FUSEKI_URL,
                 dataset: str = DEFAULT_DATASET, user: str = DEFAULT_USER,
                 password: str = DEFAULT_PASSWORD, timeout: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.dataset = dataset
        self.auth = (user, password)
        self.timeout = timeout

    # -- URLs --------------------------------------------------------------------

    @property
    def data_url(self) -> str:
        return f"{self.base_url}/{self.dataset}/data"

    @property
    def update_url(self) -> str:
        return f"{self.base_url}/{self.dataset}/update"

    @property
    def query_url(self) -> str:
        return f"{self.base_url}/{self.dataset}/query"

    # -- operations ----------------------------------------------------------------

    def clear_default_graph(self) -> None:
        """CLEAR the default graph (destructive — callers re-upload after)."""
        response = httpx.post(
            self.update_url, content=b"CLEAR DEFAULT", auth=self.auth,
            headers={"Content-Type": "application/sparql-update"},
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise FusekiError(f"CLEAR DEFAULT failed: HTTP {response.status_code}: "
                              f"{response.text[:200]}")

    def upload(self, turtle: str) -> None:
        """Append a Turtle document to the default graph."""
        response = httpx.post(
            self.data_url, content=turtle.encode("utf-8"), auth=self.auth,
            headers={"Content-Type": "text/turtle"}, timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise FusekiError(f"upload failed: HTTP {response.status_code}: "
                              f"{response.text[:200]}")

    def replace_all(self, turtles: list[tuple[str, str]]) -> None:
        """Replace the default graph with the given ``[(name, turtle), ...]``.

        The atomic-enough sequence for the sync loop: clear once, then upload
        every project artifact. If an upload fails midway the dataset is
        incomplete — the engine therefore re-runs ``replace_all`` on the next
        change, which self-heals.
        """
        self.clear_default_graph()
        for _name, turtle in turtles:
            self.upload(turtle)

    def triple_count(self) -> int:
        """Number of triples in the default graph (health/sanity)."""
        rows = self.select("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }")
        if not rows:
            return 0
        return int(rows[0].get("n", {}).get("value", 0))

    def project_iris(self) -> list[str]:
        """IRIs of the edo:Project resources in the default graph."""
        rows = self.select(
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
            "PREFIX edo: <https://w3id.org/edo/terms/> "
            "SELECT ?p WHERE { ?p rdf:type edo:Project }")
        return sorted(row["p"]["value"] for row in rows)

    def select(self, query: str) -> list[dict[str, Any]]:
        """Run a SPARQL SELECT, returning var->term bindings."""
        response = httpx.post(
            self.query_url, data={"query": query}, auth=self.auth,
            headers={"Accept": "application/sparql-results+json"},
            timeout=self.timeout,
        )
        if response.status_code != 200:
            raise FusekiError(f"query failed: HTTP {response.status_code}: "
                              f"{response.text[:200]}")
        return response.json().get("results", {}).get("bindings", [])
