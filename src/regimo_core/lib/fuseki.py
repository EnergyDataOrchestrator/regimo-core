"""Internal graph database (Fuseki ``/projects``) storage for regimo-core.

Mirrors the HTTP usage of ``fuseki-proto``'s ``docker.py``: Turtle upload to
``/{dataset}/data`` (default graph) and ``CLEAR DEFAULT`` via the SPARQL
update endpoint. Since REV-001 #1 the sync artifacts are skolemized (stable
project IRIs instead of blank nodes), so the engine can update a single
project in place (:meth:`upsert_project` / :meth:`remove_project`); the
whole-graph ``replace_all`` remains the fallback for unskolemized content.
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

    # -- per-project operations (REV-001 #1: skolemized, stable IRIs) ---------

    @staticmethod
    def _closure_query(iri: str) -> str:
        """SPARQL deleting every triple whose subject (or object) sits in
        ``iri``'s namespace: the IRI itself plus ``<iri>/...`` and
        ``<iri>#...`` descendants (SPARQL ``STR()`` yields the bare IRI
        string). Skolem bnodes (``<iri>#b-...``) and the nested
        ``<iri>/dataset:...`` IRIs match the subject arm; the object
        arm is a belt-and-braces guard for dangling references.
        External IRIs (e.g. SMS device definitions) are deliberately *not*
        matched — sensor definitions may be shared between projects.
        """
        iri = iri.replace("'", "")
        # DELETE WHERE only takes a quad pattern (no FILTER), so the deletion
        # template is spelled out explicitly.
        return (
            f"DELETE {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o . FILTER(ISIRI(?s) && ("
            f"STR(?s) = '{iri}' || STRSTARTS(STR(?s), '{iri}/') "
            f"|| STRSTARTS(STR(?s), '{iri}#') "
            f"|| (ISIRI(?o) && (STR(?o) = '{iri}' "
            f"|| STRSTARTS(STR(?o), '{iri}/') || STRSTARTS(STR(?o), '{iri}#'))))) }}"
        )

    def delete_project_closure(self, iri: str) -> None:
        """Delete one project's (or sensor's) triple closure from the
        default graph. Assumes skolemized artifacts — see :meth:`_closure_query`.
        """
        response = httpx.post(
            self.update_url, content=self._closure_query(iri).encode("utf-8"),
            auth=self.auth,
            headers={"Content-Type": "application/sparql-update"},
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise FusekiError(f"closure delete of <{iri}> failed: "
                              f"HTTP {response.status_code}: {response.text[:200]}")

    def upsert_project(self, project_iri: str, sensor_iri: str, turtle: str) -> None:
        """Replace one project's triples in the default graph.

        Deletes the project's closure (and the closure of its curated SMS
        sensor, whose definition triples are re-added by the new artifact),
        then uploads the fresh artifact. An upload failure leaves the dataset
        incomplete — the engine detects the drift via the artifact digest and
        re-upserts on the next round.
        """
        self.delete_project_closure(project_iri)
        if sensor_iri:
            self.delete_project_closure(sensor_iri)
        self.upload(turtle)

    def remove_project(self, project_iri: str) -> None:
        """Remove one project's triples when its RDMO project vanished.

        Only the project closure is deleted; SMS sensor definitions linger
        until the next full replace (they may be shared with other projects).
        """
        self.delete_project_closure(project_iri)

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
