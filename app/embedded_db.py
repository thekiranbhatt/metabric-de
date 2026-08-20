"""Lifecycle management for the optional self-contained PostgreSQL dashboard."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
from typing import Callable

import fasteners
import pgembed
import psycopg2
from psycopg2 import sql
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)
ProgressCallback = Callable[[str], None]
TRUTHY = {"1", "true", "yes", "on"}
DATABASES = {
    "SRC": "metabric_prod",
    "DEST": "metabric_warehouse",
}


@dataclass(frozen=True)
class EmbeddedDatabaseRuntime:
    """Keep the PGembed server handle alive for this Streamlit process."""

    server: object
    data_directory: str
    storage_kind: str
    boot_seconds: float
    rebuilt: bool


def embedded_postgres_enabled() -> bool:
    """Return whether the deployment opted into its self-contained database."""
    return os.getenv("METABRIC_EMBEDDED_POSTGRES", "false").strip().lower() in TRUTHY


def _report(progress: ProgressCallback | None, message: str) -> None:
    LOGGER.info(message)
    if progress:
        progress(message)


def _storage_root() -> tuple[Path, str]:
    configured = os.getenv("METABRIC_EMBEDDED_DATA_DIR")
    if configured:
        root = Path(configured).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        return root, "configured temporary storage"

    shared_memory = Path("/dev/shm")
    if shared_memory.is_dir() and os.access(shared_memory, os.W_OK):
        free_bytes = shutil.disk_usage(shared_memory).free
        if free_bytes >= 128 * 1024 * 1024:
            root = shared_memory / "metabric-embedded"
            root.mkdir(parents=True, exist_ok=True)
            return root, "memory-backed storage"

    root = Path(tempfile.gettempdir()) / "metabric-embedded"
    root.mkdir(parents=True, exist_ok=True)
    return root, "ephemeral storage"


def _configure_pgembed_runtime(root: Path) -> None:
    """Keep PGembed locks and sockets inside a writable deployment directory."""
    from pgembed.postgres_server import PostgresServer

    runtime = root / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    PostgresServer.runtime_path = runtime
    PostgresServer.lock_path = runtime / ".lockfile"
    PostgresServer._lock = fasteners.InterProcessLock(PostgresServer.lock_path)


def _create_databases(server) -> None:
    connection = psycopg2.connect(server.get_uri())
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for database in DATABASES.values():
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
                if cursor.fetchone() is None:
                    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    finally:
        connection.close()


def _connection_config(server, database: str) -> dict[str, str | int]:
    info = server.get_postmaster_info()
    if info.socket_dir is not None:
        host = str(info.socket_dir)
    else:
        host = info.hostname or "127.0.0.1"
    return {
        "host": host,
        "port": info.port or 5432,
        "dbname": database,
        "user": "postgres",
        "password": "",
        "connect_timeout": 5,
    }


def _publish_connection_environment(configs: dict[str, dict]) -> None:
    for prefix, config in configs.items():
        os.environ[f"{prefix}_DB_HOST"] = str(config["host"])
        os.environ[f"{prefix}_DB_PORT"] = str(config["port"])
        os.environ[f"{prefix}_DB_NAME"] = str(config["dbname"])
        os.environ[f"{prefix}_DB_USER"] = str(config["user"])
        os.environ[f"{prefix}_DB_PASSWORD"] = str(config["password"])


def _relation_has_rows(config: dict, relation: str) -> bool:
    connection = None
    try:
        connection = psycopg2.connect(**config)
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s)", (f"public.{relation}",))
            if cursor.fetchone()[0] is None:
                return False
            cursor.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM {} LIMIT 1)").format(sql.Identifier(relation)))
            return cursor.fetchone()[0]
    except psycopg2.Error:
        return False
    finally:
        if connection is not None:
            connection.close()


def _databases_ready(configs: dict[str, dict]) -> bool:
    return _relation_has_rows(configs["SRC"], "silver_patients") and _relation_has_rows(
        configs["DEST"], "fact_clinical_outcomes_v2"
    )


def _build_databases(configs: dict[str, dict], progress: ProgressCallback | None) -> None:
    project_root = str(PROJECT_ROOT)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from run_migrations import run_all_migrations
    from run_pipeline import run_pipeline
    from scripts.load_staging import bootstrap_staging_bronze

    _report(progress, "Applying OLTP and warehouse migrations…")
    run_all_migrations("oltp", configs["SRC"])
    run_all_migrations("warehouse", configs["DEST"])

    _report(progress, "Loading the compressed METABRIC source snapshot…")
    seed_path = PROJECT_ROOT / "data" / "staging_metabric.csv.gz"
    bootstrap_staging_bronze(configs["SRC"], csv_path=seed_path)

    _report(progress, "Building Silver and Gold models (7,000 records)…")
    run_pipeline(mode="full", src_config=configs["SRC"], dest_config=configs["DEST"])


@st.cache_resource(show_spinner=False)
def start_embedded_postgres() -> EmbeddedDatabaseRuntime:
    """Start PGembed once, populate it when needed, and expose local DB settings."""
    started = time.monotonic()
    root, storage_kind = _storage_root()
    _configure_pgembed_runtime(root)

    _report(None, f"Starting PostgreSQL 17 in {storage_kind}…")
    data_directory = root / "pgdata"
    server = pgembed.get_server(data_directory, cleanup_mode="stop")
    _create_databases(server)

    configs = {
        prefix: _connection_config(server, database)
        for prefix, database in DATABASES.items()
    }
    _publish_connection_environment(configs)

    rebuilt = not _databases_ready(configs)
    if rebuilt:
        _build_databases(configs, None)
    else:
        _report(None, "Reusing the prepared embedded warehouse…")

    return EmbeddedDatabaseRuntime(
        server=server,
        data_directory=str(data_directory),
        storage_kind=storage_kind,
        boot_seconds=time.monotonic() - started,
        rebuilt=rebuilt,
    )
