"""``regimo-core`` command line interface (CF1).

Main command: ``sync`` — pull the RDMO instance (project list via the REST
API, maDMPs via the export-plugin endpoint), keep the internal graph database
(Fuseki ``/projects``) in step and announce every change on the databus
(topic ``edo.projects.v1``). See ``regimo-core sync --help`` for all options;
every option defaults to an environment variable so compose/dev setups stay
declarative.
"""

from __future__ import annotations

import logging
import os
import time

import click

from regimo_core.lib.fuseki import DEFAULT_DATASET, DEFAULT_FUSEKI_URL, DEFAULT_PASSWORD, \
    DEFAULT_USER, FusekiStore
from regimo_core.lib.rdmo import DEFAULT_RDMO_URL, RDMOClient
from regimo_core.sync.engine import SyncEngine
from regimo_core.sync.events import PROJECTS_TOPIC, optional_bus_publisher
from regimo_core.sync.state import SyncState

LOGGER_NAME = "regimo_core"


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="regimo-core", prog_name="regimo-core")
def cli() -> None:
    """regimo-core — the orchestrator's internal engine (bus + RDMO sync)."""


@cli.command("sync")
@click.option("--rdmo-url", default=None, metavar="URL",
              help="RDMO instance base URL [env: RDMO_URL].")
@click.option("--token", default=None, metavar="TOKEN",
              help="RDMO API token [env: RDMO_API_TOKEN].")
@click.option("--fuseki-url", default=None, metavar="URL",
              help="Internal graph database base URL [env: FUSEKI_URL].")
@click.option("--dataset", default=None, metavar="NAME",
              help="Fuseki dataset holding the project graphs [env: FUSEKI_DATASET].")
@click.option("--fuseki-user", default=None, metavar="USER",
              help="Fuseki basic-auth user [env: FUSEKI_USER].")
@click.option("--fuseki-password", default=None, metavar="PASSWORD",
              help="Fuseki basic-auth password [env: FUSEKI_PASSWORD].")
@click.option("--kafka", "kafka_servers", default=None, metavar="SERVERS",
              help="Kafka bootstrap servers for the change events "
                   "[env: KAFKA_BOOTSTRAP_SERVERS]; empty disables publishing.")
@click.option("--state-file", "state_file", default=None, type=click.Path(dir_okay=False),
              help="Sync state JSON [env: REGIMO_SYNC_STATE; default data/regimo-core/state.json].")
@click.option("--artifacts-dir", "artifacts_dir", default=None, type=click.Path(file_okay=False),
              help="Per-project Turtle artifacts [env: REGIMO_SYNC_ARTIFACTS; "
                   "default data/regimo-core/artifacts].")
@click.option("--sms-extras-dir", "sms_extras_dir", default=None, type=click.Path(file_okay=False),
              help="Curated per-project SMS records (finding F12): "
                   "<dir>/<project_id>/device.json + property-*.json "
                   "[env: REGIMO_SMS_EXTRAS; default data/regimo-core/sms].")
@click.option("--interval", default=60.0, show_default=True, metavar="SECONDS",
              help="Polling interval of the loop (used with --loop).")
@click.option("--loop/--once", "run_loop", default=False, show_default=True,
              help="Run once and exit, or poll until interrupted.")
@click.option("-v", "--verbose", is_flag=True, help="Log at INFO instead of WARNING.")
def sync(rdmo_url, token, fuseki_url, dataset, fuseki_user, fuseki_password,
         kafka_servers, state_file, artifacts_dir, sms_extras_dir, interval,
         run_loop, verbose):
    """Pull RDMO, update the internal database, publish change events."""
    logging.basicConfig(level=logging.INFO if verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    rdmo_url = rdmo_url or _env("RDMO_URL", DEFAULT_RDMO_URL)
    token = token or _env("RDMO_API_TOKEN")
    if not token:
        raise click.ClickException(
            "No RDMO API token — pass --token or set RDMO_API_TOKEN "
            "(create one in the instance: env/bin/python manage.py drf_create_token <user>)")
    fuseki_url = fuseki_url or _env("FUSEKI_URL", DEFAULT_FUSEKI_URL)
    dataset = dataset or _env("FUSEKI_DATASET", DEFAULT_DATASET)
    fuseki_user = fuseki_user or _env("FUSEKI_USER", DEFAULT_USER)
    fuseki_password = fuseki_password or _env("FUSEKI_PASSWORD", DEFAULT_PASSWORD)
    kafka_servers = kafka_servers if kafka_servers is not None \
        else _env("KAFKA_BOOTSTRAP_SERVERS")
    state_file = state_file or _env(
        "REGIMO_SYNC_STATE", "data/regimo-core/state.json")
    artifacts_dir = artifacts_dir or _env(
        "REGIMO_SYNC_ARTIFACTS", "data/regimo-core/artifacts")
    sms_extras_dir = sms_extras_dir or _env(
        "REGIMO_SMS_EXTRAS", "data/regimo-core/sms")

    client = RDMOClient(rdmo_url, token)
    store = FusekiStore(fuseki_url, dataset, fuseki_user, fuseki_password)
    state = SyncState(path=_path(state_file)).load()
    engine = SyncEngine(
        client=client,
        store=store,
        state=state,
        artifacts_dir=_path(artifacts_dir),
        bus=optional_bus_publisher(kafka_servers),
        sms_extras_dir=_path(sms_extras_dir),
    )

    click.echo(f"regimo-core sync: rdmo={rdmo_url} fuseki={fuseki_url}/{dataset} "
               f"kafka={kafka_servers or '(disabled)'}")
    while True:
        result = _run_round(engine, dataset)
        if not run_loop:
            break
        time.sleep(interval)


def _run_round(engine: SyncEngine, dataset: str) -> None:
    from regimo_core.sync.transform import TransformError

    try:
        result = engine.run_once()
    except Exception as exc:
        logging.getLogger(LOGGER_NAME).error("sync round failed: %s", exc)
        click.echo(f"sync round failed: {exc}", err=True)
        return
    if result.created:
        click.echo(f"created: {result.created}")
    if result.updated:
        click.echo(f"updated: {result.updated}")
    if result.removed:
        click.echo(f"removed: {result.removed}")
    for project_id, reason in result.skipped:
        click.echo(f"skipped project {project_id}: {reason}")
    if result.graph_replaced:
        count = engine.store.triple_count()
        click.echo(f"default graph replaced ({count} triples in /{dataset})")
    if not result.changed:
        click.echo("no changes")


def _path(value: str):
    from pathlib import Path
    return Path(value).expanduser()
