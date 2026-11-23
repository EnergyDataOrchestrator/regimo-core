"""Deterministic skolemization of blank nodes (REV-001, recommendation #1).

The internal format is serialised with blank nodes (rdflib assigns fresh
labels on every parse), which is why the sync engine could only *replace*
the whole Fuseki default graph instead of updating one project. Replacing
every blank node with a stable IRI keyed to the project makes per-project
deletion/upsert possible and gives every artifact a meaningful content
digest.

Stability contract: the skolem IRI is ``<project_iri>#b-<16 hex>`` where the
hex is the leading part of the SHA-256 of the bnode's canonical subtree
signature (all outgoing arcs, nested blank nodes by their own signature).
The signature derives purely from graph content — never from rdflib's
randomly generated bnode labels — so two parses of the same document produce
the same IRIs. Structurally identical bnodes receive the same IRI, which is
sound for acyclic graphs (blank nodes are existentials; identical subtrees
may be unified). Cyclic structures raise :class:`SkolemizationError` and the
engine falls back to the whole-graph replace path.
"""

from __future__ import annotations

import hashlib

from rdflib import BNode, Graph, URIRef


class SkolemizationError(RuntimeError):
    """The blank-node structure is cyclic and cannot be canonicalised."""


def skolemize(turtle: str, project_iri: str) -> str:
    """Replace every blank node in ``turtle`` with a stable project IRI.

    A no-op when ``project_iri`` is empty or the graph has no blank nodes
    (which also makes the function idempotent: skolemized output contains no
    blank nodes, so re-running returns the input unchanged).
    """
    if not project_iri:
        return turtle
    graph = Graph()
    graph.parse(data=turtle, format="turtle")
    bnodes = {node for triple in graph for node in triple if isinstance(node, BNode)}
    if not bnodes:
        return turtle

    signatures: dict[BNode, str] = {}
    in_progress: set[BNode] = set()

    def render(node: object) -> str:
        if isinstance(node, BNode):
            return f"_:{signature(node)}"
        return node.n3()

    def signature(node: BNode) -> str:
        if node in signatures:
            return signatures[node]
        if node in in_progress:
            raise SkolemizationError(f"cyclic blank-node structure at {node}")
        in_progress.add(node)
        try:
            lines = [
                f"{predicate.n3()} {render(obj)}"
                for predicate, obj in sorted(
                    graph.predicate_objects(node),
                    key=lambda pair: (str(pair[0]), render(pair[1])),
                )
            ]
        finally:
            in_progress.discard(node)
        digest = hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()
        signatures[node] = digest
        return digest

    replacements = {
        node: URIRef(f"{project_iri}#b-{signature(node)[:16]}") for node in bnodes
    }

    skolemized = Graph()
    for subject, predicate, obj in graph:
        skolemized.add((
            replacements.get(subject, subject),  # type: ignore[arg-type]
            predicate,
            replacements.get(obj, obj),  # type: ignore[arg-type]
        ))
    return skolemized.serialize(format="turtle")
