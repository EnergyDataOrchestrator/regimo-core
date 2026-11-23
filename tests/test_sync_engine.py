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
    """Records upserts/removals/replacements instead of touching Fuseki."""

    def __init__(self):
        self.replacements: list[list[tuple[str, str]]] = []
        self.upserts: list[tuple[str, str, str]] = []  # (project_iri, sensor, turtle)
        self.removals: list[str] = []

    def replace_all(self, turtles):
        self.replacements.append(list(turtles))

    def upsert_project(self, project_iri, sensor_iri, turtle):
        self.upserts.append((project_iri, sensor_iri, turtle))

    def remove_project(self, project_iri):
        self.removals.append(project_iri)


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


def failing_transform(document, device=None, properties=None):
    """A transform whose meta lacks project_iri (pre-skolemization shape)."""
    turtle, _meta = fake_transform(document)
    return turtle, {}


@pytest.fixture
def setup(tmp_path: Path):
    """(engine helpers, store, bus, state, artifacts_dir)."""
    store, bus = FakeStore(), FakeBus()
    state = SyncState(path=tmp_path / "state.json")
    artifacts = tmp_path / "artifacts"
    return store, bus, state, artifacts


def _engine(client, store, bus, state, artifacts, transform=fake_transform,
            **kwargs):
    return SyncEngine(client=client, store=store, state=state,
                      artifacts_dir=artifacts, transform=transform, bus=bus,
                      **kwargs)


def test_first_run_upserts_each_skolemized_project(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        5: {"dmp": {"title": "B", "dmp_id": {"identifier": "urn:uuid:5"}}},
    })
    engine = _engine(client, store, bus, state, artifacts)

    result = engine.run_once()

    assert result.created == [4, 5]
    assert result.changed and result.store_mode == "upsert"
    assert not result.graph_replaced
    assert not store.replacements
    # one per-project upsert per created project, no whole-graph replace
    assert [(iri, sensor) for iri, sensor, _ in store.upserts] == [
        ("urn:uuid:4", ""), ("urn:uuid:5", "")]
    kinds = sorted(event.kind for event in result.events)
    assert kinds == ["project.created", "project.created"]
    # every project event on the bus, headers carry the regimo-core contract
    projects = [pub for pub in bus.published if pub[0] == PROJECTS_TOPIC]
    assert len(projects) == 2
    assert all(h["socket_id"] == "regimo-core" for _, _, h in projects)
    # plus one round diagnostic (heartbeat) on the sync topic
    assert [pub[0] for pub in bus.published if pub[0] == SYNC_TOPIC] == [SYNC_TOPIC]
    # state persisted with digests + artifact name
    state4 = state.get(4)
    assert state4.madmp_sha256 == madmp_digest(client.madmps[4])
    assert state4.graph_sha256
    assert state4.artifact == "4.ttl"
    assert state4.project_iri == "urn:uuid:4"


def test_event_payload_carries_the_digests(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({4: {"dmp": {"title": "M2",
                                         "dmp_id": {"identifier": "urn:uuid:4"}}}})
    engine = _engine(client, store, bus, state, artifacts)
    engine.run_once()

    projects = [pub for pub in bus.published if pub[0] == PROJECTS_TOPIC]
    payload = projects[0][1]
    # both digests present and consistent with the persisted state; the graph
    # digest is taken over the skolemized artifact, not the raw maDMP
    assert len(payload["project"]["plan_digest_sha256"]) == 64
    assert payload["project"]["plan_digest_sha256"] == state.get(4).madmp_sha256
    assert payload["project"]["graph_sha256"] == state.get(4).graph_sha256


def test_unskolemized_project_falls_back_to_replace_all(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({4: {"dmp": {"title": "M2",
                                         "dmp_id": {"identifier": "urn:uuid:4"}}}})
    engine = _engine(client, store, bus, state, artifacts,
                     transform=failing_transform)

    result = engine.run_once()

    assert result.created == [4]
    assert result.store_mode == "replace" and result.graph_replaced
    assert len(store.replacements) == 1


def test_second_run_is_a_no_op(setup):
    store, bus, state, artifacts = setup
    madmp = {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}}
    client = FakeRDMOClient({4: madmp})
    engine = _engine(client, store, bus, state, artifacts)

    engine.run_once()
    second = engine.run_once()

    assert not second.changed
    assert second.events == []
    assert len(store.upserts) == 1  # no second upsert
    # no further project events, but the round heartbeat is published anyway
    projects = [pub for pub in bus.published if pub[0] == PROJECTS_TOPIC]
    rounds = [pub for pub in bus.published if pub[0] == SYNC_TOPIC]
    assert len(projects) == 1 and len(rounds) == 2


def test_changed_madmp_upserts_only_the_changed_project(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        5: {"dmp": {"title": "B", "dmp_id": {"identifier": "urn:uuid:5"}}},
    })
    engine = _engine(client, store, bus, state, artifacts)
    engine.run_once()
    upserts_after_first = len(store.upserts)

    client.madmps[4] = {"dmp": {"title": "M2 changed",
                                "dmp_id": {"identifier": "urn:uuid:4"}}}
    second = engine.run_once()

    assert second.updated == [4]
    assert second.created == []
    assert second.events[0].kind == "project.updated"
    assert second.store_mode == "upsert"
    assert len(store.upserts) == upserts_after_first + 1
    assert store.upserts[-1][0] == "urn:uuid:4"


def test_removed_project_closes_its_triples_and_announces(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        5: {"dmp": {"title": "B", "dmp_id": {"identifier": "urn:uuid:5"}}},
    })
    engine = _engine(client, store, bus, state, artifacts)
    engine.run_once()
    upserts_after_first = len(store.upserts)

    del client.madmps[5]
    second = engine.run_once()

    assert second.removed == [5]
    assert second.events[-1].kind == "project.removed"
    assert not (artifacts / "5.ttl").exists()
    # the removed project's closure is deleted; the survivor is not touched
    assert store.removals == ["urn:uuid:5"]
    assert len(store.upserts) == upserts_after_first
    assert state.known_ids() == {4}


def test_artifact_drift_triggers_retransform_and_upsert(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({4: {"dmp": {"title": "M2",
                                         "dmp_id": {"identifier": "urn:uuid:4"}}}})
    engine = _engine(client, store, bus, state, artifacts)
    engine.run_once()
    assert len(store.upserts) == 1

    # someone clobbers the artifact file (failed upload, manual edit, ...)
    (artifacts / "4.ttl").write_text("<urn:uuid:4> <urn:vocab:x> \"broken\" .\n",
                                     encoding="utf-8")
    second = engine.run_once()

    assert second.updated == [4]  # re-transformed even though the maDMP is unchanged
    assert len(store.upserts) == 2
    assert "<urn:vocab:x>" not in store.upserts[-1][2]


def test_full_replace_pending_forces_replace_even_without_changes(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({4: {"dmp": {"title": "M2",
                                         "dmp_id": {"identifier": "urn:uuid:4"}}}})
    engine = _engine(client, store, bus, state, artifacts)
    engine.run_once()
    state.full_replace_pending = True

    second = engine.run_once()

    assert not second.changed
    assert second.store_mode == "replace" and second.graph_replaced
    assert len(store.replacements) == 1
    assert state.full_replace_pending is False  # cleared on success


def test_failing_export_and_transform_are_skipped(setup):
    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        7: None,  # export raises
        8: {"dmp": {"title": "X", "dmp_id": {"identifier": "urn:uuid:8"}}},
    })

    def _failing_transform(document, device=None, properties=None):
        if document["dmp"]["title"] == "X":
            raise RuntimeError("bad map")
        return fake_transform(document)

    engine = _engine(client, store, bus, state, artifacts,
                     transform=_failing_transform)

    result = engine.run_once()

    assert result.created == [4]
    assert (7, "maDMP export failed: maDMP export of project 7 failed") in result.skipped
    assert any(pid == 8 and "transform failed" in reason
               for pid, reason in result.skipped)
    # project 8 was never stored, so only project 4 was upserted
    assert [iri for iri, _, _ in store.upserts] == ["urn:uuid:4"]


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
    engine = _engine(client, store, bus, state, artifacts,
                     transform=capture_transform,
                     sms_extras_dir=tmp_path / "sms")
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
    engine = _engine(client, store, bus, state, artifacts)
    engine.run_once()

    reloaded = SyncState(path=state.path).load()
    assert reloaded.get(4).madmp_sha256 == state.get(4).madmp_sha256
    assert reloaded.get(4).graph_sha256 == state.get(4).graph_sha256
    assert reloaded.get(4).artifact == "4.ttl"


def test_event_payload_and_header(setup):
    event = SyncEvent(kind="project.created", project_id=4, title="M2",
                      project_iri="urn:uuid:4",
                      plan_digest_sha256="ab" * 32, graph_sha256="cd" * 32)
    payload = event.payload()
    assert payload["event"] == "project.created"
    assert payload["project"]["project_iri"] == "urn:uuid:4"
    assert payload["project"]["plan_digest_sha256"] == "ab" * 32
    assert payload["project"]["graph_sha256"] == "cd" * 32
    header = event.header()
    assert header.socket_id == "regimo-core"
    assert header.project_id == "4"
    assert header.schema_id == "edo.projects.v1"
    assert "agnostic-trip" in header.lineage


def test_round_event_announces_stats_skips_and_plans(setup):
    """The heartbeat carries the round outcome, skip reasons and the
    per-project digest list for the ADP badge."""
    from regimo_core.sync.events import publish_round_event

    store, bus, state, artifacts = setup
    client = FakeRDMOClient({
        4: {"dmp": {"title": "M2", "dmp_id": {"identifier": "urn:uuid:4"}}},
        7: None,  # export raises -> skipped
    })
    engine = _engine(client, store, bus, state, artifacts)
    engine.run_once()

    rounds = [pub for pub in bus.published if pub[0] == SYNC_TOPIC]
    assert len(rounds) == 1
    _, payload, header = rounds[0]
    assert payload["event"] == "sync.round"
    assert payload["stats"] == {"created": 1, "updated": 0, "removed": 0,
                                "skipped": 1, "graph_replaced": False,
                                "store_mode": "upsert"}
    assert payload["skipped"][0]["project_id"] == 7
    assert "maDMP export failed" in payload["skipped"][0]["reason"]
    assert payload["plans"] == [{
        "project_id": "4",
        "title": "Project 4",
        "project_iri": "urn:uuid:4",
        "plan_digest_sha256": state.get(4).madmp_sha256,
        "graph_sha256": state.get(4).graph_sha256,
    }]
    assert isinstance(payload["duration_ms"], int)
    assert header["socket_id"] == "regimo-core"
    assert header["schema_id"] == SYNC_TOPIC
    assert header["project_id"] == ""  # round event is not bound to a project


def test_round_event_without_bus_is_a_noop():
    from regimo_core.sync.events import publish_round_event

    assert publish_round_event(None, None) is False
