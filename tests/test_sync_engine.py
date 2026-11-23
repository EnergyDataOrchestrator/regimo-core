"""Unit tests for the RDMO -> internal-database sync engine (no network)."""

import json
from pathlib import Path

import pytest

from regimo_core.lib.rdmo import RDMOProject
from regimo_core.sync.engine import SyncEngine, SyncState, madmp_digest
from regimo_core.sync.events import PROJECTS_TOPIC, SYNC_TOPIC, SyncEvent


class FakeRDMOClient:
    """Stand-in for RDMOClient with an in-memory project store."""

    def __init__(self, madmps: dict[int, dict]):
        self.madmps = madmps
        self.calls = 0

    def list_projects(self):
        self.calls += 1
        return [
            RDMOProject(id=pid, title=f"Project {pid}", description="",
                        created="2026-01-01", updated="2026-01-02",
                        last_changed="2026-01-02")
            for pid in sorted(self.madmps)
        ]

    def get_madmp(self, project_id):
        document = self.madmps[project_id]
        if document is None:
            from regimo_core.lib.rdmo import RDMOError
            raise RDMOError(f"maDMP export of project {project_id} failed")
        return document


class FakeStore:
    def __init__(self):
        self.replacements: list[list[tuple[str, str]]] = []

    def replace_all(self, turtles):
        self.replacements.append(list(turtles))


class FakeBus:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload, header):
        self.published.append((topic, payload, header.to_dict()))


def fake_transform(document, device=None, properties=None):
    """Deterministic 'transform': one turtle line per maDMP title."""
    dmp_id = document["dmp"]["dmp_id"]["identifier"]
    turtle = f"<{dmp_id}> <http://purl.org/dc/terms/title> \"Project\" .\n"
    return turtle, {"project_iri": dmp_id}


@pytest.fixture
def setup(tmp_path: Path):
    """(engine helpers, store, bus, state, artifacts_dir)."""
    store, bus = FakeStore(), FakeBus()
    state = SyncState(path=tmp_path / "state.json")
    artifacts = tmp_path / "artifacts"
    return store, bus, state, artifacts


def test_first_run_creates_all_and_replaces_graph(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        5: {"dmp": {"title": "B", "dmp_id": {"identifier": "urn:uuid:5"}}},
    })
    engine = SyncEngine(client=client, store=store, state=state,
                        artifacts_dir=artifacts, transform=fake_transform, bus=bus)

    result = engine.run_once()

    assert result.created == [4, 5]
    assert result.changed and result.graph_replaced
    assert len(store.replacements) == 1
    assert {name for name, _ in store.replacements[0]} == {"4.ttl", "5.ttl"}
    kinds = sorted(event.kind for event in result.events)
    assert kinds == ["project.created", "project.created"]
    # every project event on the bus, headers carry the regimo-core contract
    projects = [pub for pub in bus.published if pub[0] == PROJECTS_TOPIC]
    assert len(projects) == 2
    assert all(h["socket_id"] == "regimo-core" for _, _, h in projects)
    # plus one round diagnostic (heartbeat) on the sync topic
    assert [pub[0] for pub in bus.published if pub[0] == SYNC_TOPIC] == [SYNC_TOPIC]
    # state persisted with digest + artifact name
    state4 = state.get(4)
    assert state4.madmp_sha256 == madmp_digest(client.madmps[4])
    assert state4.artifact == "4.ttl"
    assert state4.project_iri == "urn:uuid:4"


def test_second_run_is_a_no_op(setup):
    store, bus, state, artifacts = setup
    madmp = {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}}
    client = FakeRDMOClient({4: madmp})
    engine = SyncEngine(client=client, store=store, state=state,
                        artifacts_dir=artifacts, transform=fake_transform, bus=bus)

    engine.run_once()
    second = engine.run_once()

    assert not second.changed
    assert second.events == []
    assert len(store.replacements) == 1  # no second replace
    # no further project events, but the round heartbeat is published anyway
    projects = [pub for pub in bus.published if pub[0] == PROJECTS_TOPIC]
    rounds = [pub for pub in bus.published if pub[0] == SYNC_TOPIC]
    assert len(projects) == 1 and len(rounds) == 2


def test_changed_madmp_updates(setup):
    store, bus, state, artifacts = setup
    madmp = {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}}
    client = FakeRDMOClient({4: madmp})
    engine = SyncEngine(client=client, store=store, state=state,
                        artifacts_dir=artifacts, transform=fake_transform, bus=bus)
    engine.run_once()

    client.madmps[4] = {"dmp": {"title": "M2 changed",
                                "dmp_id": {"identifier": "urn:uuid:4"}}}
    second = engine.run_once()

    assert second.updated == [4]
    assert second.created == []
    assert second.events[0].kind == "project.updated"
    assert len(store.replacements) == 2


def test_removed_project_forgets_artifact_and_announces(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        5: {"dmp": {"title": "B", "dmp_id": {"identifier": "urn:uuid:5"}}},
    })
    engine = SyncEngine(client=client, store=store, state=state,
                        artifacts_dir=artifacts, transform=fake_transform, bus=bus)
    engine.run_once()

    del client.madmps[5]
    second = engine.run_once()

    assert second.removed == [5]
    assert second.events[-1].kind == "project.removed"
    assert not (artifacts / "5.ttl").exists()
    # the replacement graph no longer contains the removed project
    names = {name for name, _ in store.replacements[-1]}
    assert names == {"4.ttl"}
    assert state.known_ids() == {4}


def test_failing_export_and_transform_are_skipped(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        7: None,  # export raises
        8: {"dmp": {"title": "X", "dmp_id": {"identifier": "urn:uuid:8"}}},
    })
    def failing_transform(document, device=None, properties=None):
        if document["dmp"]["title"] == "X":
            raise RuntimeError("bad map")
        return fake_transform(document)

    engine = SyncEngine(client=client, store=store, state=state,
                        artifacts_dir=artifacts, transform=failing_transform,
                        bus=bus)

    result = engine.run_once()

    assert result.created == [4]
    assert (7, "maDMP export failed: maDMP export of project 7 failed") in result.skipped
    assert any(pid == 8 and "transform failed" in reason
               for pid, reason in result.skipped)
    # project 8 was never stored, so the graph holds only project 4
    assert [name for name, _ in store.replacements[0]] == ["4.ttl"]


def test_sms_extras_are_passed_to_the_transform(setup, tmp_path):
    store, bus, state, artifacts = setup
    extras = tmp_path / "sms" / "4"
    extras.mkdir(parents=True)
    (extras / "device.json").write_text(json.dumps({"data": {"id": "67"}}),
                                        encoding="utf-8")
    (extras / "property-157.json").write_text(
        json.dumps({"data": {"id": "157"}}), encoding="utf-8")
    seen = {}

    def capture_transform(document, device=None, properties=None):
        seen["device"] = device
        seen["properties"] = properties
        return fake_transform(document)

    client = FakeRDMOClient({4: {"dmp": {"title": "M2",
                                         "dmp_id": {"identifier": "urn:uuid:4"}}}})
    engine = SyncEngine(client=client, store=store, state=state,
                        artifacts_dir=artifacts, transform=capture_transform,
                        bus=bus, sms_extras_dir=tmp_path / "sms")
    engine.run_once()

    assert seen["device"] == {"data": {"id": "67"}}
    assert seen["properties"] == [{"data": {"id": "157"}}]

    # project without an extras directory runs DMP-only
    seen.clear()
    client.madmps[4] = {"dmp": {"title": "M2 changed",
                                "dmp_id": {"identifier": "urn:uuid:4"}}}
    engine.sms_extras_dir = None
    engine.run_once()
    assert seen["device"] is None and seen["properties"] == []


def test_state_roundtrip(setup, tmp_path):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({4: {"dmp": {"title": "M2",
                                         "dmp_id": {"identifier": "urn:uuid:4"}}}})
    engine = SyncEngine(client=client, store=store, state=state,
                        artifacts_dir=artifacts, transform=fake_transform, bus=bus)
    engine.run_once()

    reloaded = SyncState(path=state.path).load()
    assert reloaded.get(4).madmp_sha256 == state.get(4).madmp_sha256
    assert reloaded.get(4).artifact == "4.ttl"


def test_event_payload_and_header(setup):
    event = SyncEvent(kind="project.created", project_id=4, title="M2",
                      project_iri="urn:uuid:4")
    payload = event.payload()
    assert payload["event"] == "project.created"
    assert payload["project"]["project_iri"] == "urn:uuid:4"
    header = event.header()
    assert header.socket_id == "regimo-core"
    assert header.project_id == "4"
    assert header.schema_id == "edo.projects.v1"
    assert "agnostic-trip" in header.lineage


def test_round_event_announces_stats_and_skips(setup):
    """The heartbeat carries the round outcome — including skip reasons."""
    from regimo_core.sync.events import publish_round_event

    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        7: None,  # export raises -> skipped
    })
    engine = SyncEngine(client=client, store=store, state=state,
                        artifacts_dir=artifacts, transform=fake_transform, bus=bus)
    engine.run_once()

    rounds = [pub for pub in bus.published if pub[0] == SYNC_TOPIC]
    assert len(rounds) == 1
    _, payload, header = rounds[0]
    assert payload["event"] == "sync.round"
    assert payload["stats"] == {"created": 1, "updated": 0, "removed": 0,
                                "skipped": 1, "graph_replaced": True}
    assert payload["skipped"][0]["project_id"] == 7
    assert "maDMP export failed" in payload["skipped"][0]["reason"]
    assert isinstance(payload["duration_ms"], int)
    assert header["socket_id"] == "regimo-core"
    assert header["schema_id"] == SYNC_TOPIC
    assert header["project_id"] == ""  # round event is not bound to a project


def test_round_event_without_bus_is_a_noop():
    from regimo_core.sync.events import publish_round_event

    assert publish_round_event(None, None) is False
