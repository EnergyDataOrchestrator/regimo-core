"""Tests for the deterministic blank-node skolemizer (REV-001 #1)."""

import pytest
from rdflib import BNode, Graph

from regimo_core.sync.skolemize import SkolemizationError, skolemize

PROJECT = "urn:uuid:d9e575a1-1111-2222-3333-444455556666"

# rdflib generates fresh blank-node labels for every parse of this document —
# the skolem IRIs must not depend on them.
BNODE_DOC = """
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix edo: <https://w3id.org/edo/terms/> .

<urn:uuid:p1> dcterms:title "Weatherstation" ;
    edo:contact [ edo:identifier_value "jane@example.org" ;
                  dcterms:type "Contact" ] ;
    edo:distribution [ dcterms:title "measurements 2026" ] .
"""


def _triples(turtle: str) -> set[tuple[str, str, str]]:
    graph = Graph()
    graph.parse(data=turtle, format="turtle")
    return {
        (s.n3(), p.n3(), o.n3()) for s, p, o in graph
    }


def test_deterministic_across_parses():
    """Two runs on the same input (fresh bnode labels each parse) agree."""
    first = skolemize(BNODE_DOC, PROJECT)
    second = skolemize(BNODE_DOC, PROJECT)
    assert first == second


def test_all_blank_nodes_replaced_with_project_scoped_iris():
    out = skolemize(BNODE_DOC, PROJECT)
    graph = Graph()
    graph.parse(data=out, format="turtle")
    nodes = {node for triple in graph for node in triple}
    assert not [node for node in nodes if isinstance(node, BNode)]
    skolems = {str(node) for node in nodes if "#b-" in str(node)}
    assert skolems and all(iri.startswith(PROJECT + "#b-") for iri in skolems)


def test_idempotent():
    once = skolemize(BNODE_DOC, PROJECT)
    twice = skolemize(once, PROJECT)
    assert once == twice


def test_no_project_iri_is_a_noop():
    assert skolemize(BNODE_DOC, "") == BNODE_DOC


def test_identical_subtrees_merge_to_one_iri():
    # two leaf bnodes with identical (empty) descriptions are unified —
    # sound for acyclic graphs, where blank nodes are existentials
    doc = '<urn:uuid:p1> <urn:vocab:p> [], [] .\n'
    out = skolemize(doc, PROJECT)
    graph = Graph()
    graph.parse(data=out, format="turtle")
    p_triples = [o for s, p, o in graph
                 if str(p) == "urn:vocab:p"]
    assert len(p_triples) == 1


def test_different_subtrees_stay_distinct():
    doc = """
    @prefix edo: <https://w3id.org/edo/terms/> .
    <urn:uuid:p1> edo:a [ edo:x "1" ] ; edo:b [ edo:x "2" ] .
    """
    out = skolemize(doc, PROJECT)
    graph = Graph()
    graph.parse(data=out, format="turtle")
    objects = {str(o) for s, p, o in graph
               if str(p) in ("https://w3id.org/edo/terms/a",
                             "https://w3id.org/edo/terms/b")}
    assert len(objects) == 2


def test_cross_project_isolation():
    doc = '<urn:uuid:p1> <urn:vocab:p> [ <urn:vocab:q> "v" ] .\n'
    ta = _triples(skolemize(doc, "urn:uuid:aaaa"))
    tb = _triples(skolemize(doc, "urn:uuid:bbbb"))
    assert ta != tb
    assert all("urn:uuid:aaaa#b-" in "".join(t) for t in ta if "#b-" in "".join(t))
    assert any("urn:uuid:bbbb#b-" in "".join(t) for t in tb)


def test_cyclic_structure_raises():
    doc = """
    @prefix edo: <https://w3id.org/edo/terms/> .
    <urn:uuid:p1> edo:a [ edo:next _:b ] .
    _:b edo:next _:b .
    """
    with pytest.raises(SkolemizationError):
        skolemize(doc, PROJECT)


def test_nested_blank_nodes_get_distinct_stable_iris():
    doc = """
    @prefix edo: <https://w3id.org/edo/terms/> .
    <urn:uuid:p1> edo:outer [ edo:inner [ edo:x "deep" ] ] .
    """
    out = skolemize(doc, PROJECT)
    graph = Graph()
    graph.parse(data=out, format="turtle")
    iris = {str(n) for triple in graph for n in triple if "#b-" in str(n)}
    assert len(iris) == 2  # outer + inner, both stable content hashes
    assert skolemize(out, PROJECT) == out
