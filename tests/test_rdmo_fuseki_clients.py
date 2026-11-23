"""Tests for the RDMO client and the Fuseki store (httpx MockTransport)."""

import json

import httpx
import pytest

from regimo_core.lib.fuseki import FusekiError, FusekiStore
from regimo_core.lib.rdmo import RDMOClient, RDMOError


def _page(results, next_url=None):
    return {"count": len(results), "next": next_url, "previous": None,
            "results": results}


def test_list_projects_follows_pagination():
    def handler(request: httpx.Request) -> httpx.Response:
        if "page=2" in str(request.url):
            return httpx.Response(200, json=_page([{"id": 5, "title": "B"}]))
        assert request.url.params["page_size"] == "100"
        assert "Authorization" in request.headers
        return httpx.Response(200, json=_page([{"id": 4, "title": "M2"}],
                                              next_url="/api/v1/projects/projects/?page=2"))

    client = RDMOClient("http://rdmo.test", token="t0k3n")
    client._client = lambda: httpx.Client(
        base_url=client.base_url, headers={"Authorization": "Token t0k3n"},
        transport=httpx.MockTransport(handler))
    projects = client.list_projects()
    assert [(p.id, p.title) for p in projects] == [(4, "M2"), (5, "B")]


def test_get_madmp_returns_document():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/madmp/projects/4/"
        return httpx.Response(200, json={"dmp": {"title": "M2"}})

    client = RDMOClient("http://rdmo.test", token="t0k3n")
    client._client = lambda: httpx.Client(
        base_url=client.base_url, headers={"Authorization": "Token t0k3n"},
        transport=httpx.MockTransport(handler))
    assert client.get_madmp(4) == {"dmp": {"title": "M2"}}


def test_get_madmp_maps_error_statuses():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("3/"):
            return httpx.Response(403, json={"detail": "forbidden"})
        return httpx.Response(404)

    client = RDMOClient("http://rdmo.test", token="t0k3n")
    client._client = lambda: httpx.Client(
        base_url=client.base_url, headers={"Authorization": "Token t0k3n"},
        transport=httpx.MockTransport(handler))
    with pytest.raises(RDMOError, match="not permitted"):
        client.get_madmp(3)
    with pytest.raises(RDMOError, match="not found"):
        client.get_madmp(99)


def test_fuseki_replace_all_clears_then_uploads():
    calls: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/update"):
            calls.append(("update", request.content.decode(), ""))
            return httpx.Response(200)
        if request.url.path.endswith("/data"):
            calls.append(("data", request.headers["content-type"],
                          request.content.decode()))
            return httpx.Response(200)
        return httpx.Response(500)

    store = FusekiStore("http://fuseki.test", "projects")
    # patch the module-level httpx usage via transport on a fresh client set
    real_post = httpx.post

    def fake_post(url, **kwargs):
        kwargs.pop("timeout", None)
        kwargs.pop("auth", None)
        request = httpx.Request("POST", url, **kwargs)
        response = handler(request)
        return response

    httpx.post = fake_post
    try:
        store.replace_all([("a.ttl", "<urn:a> a <urn:P> ."),
                           ("b.ttl", "<urn:b> a <urn:P> .")])
    finally:
        httpx.post = real_post

    assert calls[0][0] == "update" and calls[0][1] == "CLEAR DEFAULT"
    assert [c[2] for c in calls[1:]] == ["<urn:a> a <urn:P> .",
                                         "<urn:b> a <urn:P> ."]
    assert all(c[1] == "text/turtle" for c in calls[1:])


def test_fuseki_upsert_project_deletes_closures_then_uploads():
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/update"):
            calls.append(("update", request.content.decode()))
            return httpx.Response(200)
        if request.url.path.endswith("/data"):
            calls.append(("data", request.content.decode()))
            return httpx.Response(200)
        return httpx.Response(500)

    real_post = httpx.post

    def fake_post(url, **kwargs):
        kwargs.pop("timeout", None)
        kwargs.pop("auth", None)
        return handler(httpx.Request("POST", url, **kwargs))

    httpx.post = fake_post
    try:
        store = FusekiStore("http://fuseki.test", "projects")
        store.upsert_project(
            "urn:uuid:4", "https://w3id.org/edo/sms/dev-67",
            "<urn:uuid:4> <urn:vocab:p> <urn:uuid:4#b-1> .")
    finally:
        httpx.post = real_post

    # first the project closure, then the sensor closure, then the upload
    assert [kind for kind, _ in calls] == ["update", "update", "data"]
    project_delete, sensor_delete = calls[0][1], calls[1][1]
    assert "STR(?s) = 'urn:uuid:4'" in project_delete
    assert "STRSTARTS(STR(?s), 'urn:uuid:4/')" in project_delete
    assert "STRSTARTS(STR(?s), 'urn:uuid:4#')" in project_delete
    assert "ISIRI(?o)" in project_delete
    assert "dev-67" in sensor_delete and "urn:uuid:4" not in sensor_delete
    assert calls[2][1].startswith("<urn:uuid:4>")


def test_fuseki_upsert_without_sensor_deletes_one_closure():
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/update"):
            calls.append(("update", request.content.decode()))
            return httpx.Response(200)
        if request.url.path.endswith("/data"):
            calls.append(("data", request.content.decode()))
            return httpx.Response(200)
        return httpx.Response(500)

    real_post = httpx.post

    def fake_post(url, **kwargs):
        kwargs.pop("timeout", None)
        kwargs.pop("auth", None)
        return handler(httpx.Request("POST", url, **kwargs))

    httpx.post = fake_post
    try:
        store = FusekiStore("http://fuseki.test", "projects")
        store.upsert_project("urn:uuid:4", "", "<urn:uuid:4> a <urn:P> .")
        store.remove_project("urn:uuid:4")
    finally:
        httpx.post = real_post

    assert [kind for kind, _ in calls] == ["update", "data", "update"]
    assert "urn:uuid:4" in calls[0][1]


def test_fuseki_closure_delete_failure_raises_fuseki_error():
    real_post = httpx.post

    def fake_post(url, **kwargs):
        kwargs.pop("timeout", None)
        kwargs.pop("auth", None)
        return httpx.Response(500, text="boom")

    httpx.post = fake_post
    try:
        store = FusekiStore("http://fuseki.test", "projects")
        with pytest.raises(FusekiError, match="closure delete"):
            store.remove_project("urn:uuid:4")
    finally:
        httpx.post = real_post


def test_fuseki_select_parses_bindings():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["accept"] == "application/sparql-results+json"
        return httpx.Response(200, json={
            "results": {"bindings": [{"p": {"type": "uri",
                                            "value": "urn:uuid:4"}}]}})

    real_post = httpx.post

    def fake_post(url, **kwargs):
        kwargs.pop("timeout", None)
        kwargs.pop("auth", None)
        return handler(httpx.Request("POST", url, **kwargs))

    httpx.post = fake_post
    try:
        store = FusekiStore("http://fuseki.test", "projects")
        rows = store.select("SELECT ?p WHERE { ?p ?x ?y }")
    finally:
        httpx.post = real_post
    assert rows[0]["p"]["value"] == "urn:uuid:4"
