import argparse
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent


def _database_config(prefix: str) -> dict[str, str | None]:
    return {
        "host": os.getenv(f"{prefix}_DB_HOST"),
        "database": os.getenv(f"{prefix}_DB_NAME"),
        "user": os.getenv(f"{prefix}_DB_USER"),
        "password": os.getenv(f"{prefix}_DB_PASSWORD"),
        "port": os.getenv(f"{prefix}_DB_PORT"),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Run migrations against the OLTP or warehouse database")
    parser.add_argument(
        "--target",
        choices=["oltp", "warehouse"],
        required=True,
        help="Which database to migrate: 'oltp' (metabric_prod) or 'warehouse' (metabric_warehouse)"
    )
    return parser.parse_args()


def run_all_migrations(target: str, config: dict | None = None):
    if target not in {"oltp", "warehouse"}:
        raise ValueError("target must be either 'oltp' or 'warehouse'")

    config = config or _database_config("SRC" if target == "oltp" else "DEST")
    migration_dir = PROJECT_ROOT / "migrations" / target

    if not migration_dir.exists():
        raise FileNotFoundError(f"Migration directory not found at {migration_dir}")

    files = sorted(path for path in migration_dir.iterdir() if path.suffix == ".sql")

    conn = psycopg2.connect(**config)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(255) UNIQUE NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        for file in files:
            cur.execute("SELECT 1 FROM schema_migrations WHERE filename = %s", (file.name,))
            if cur.fetchone():
                print(f" Skipping already applied migration: {file.name}")
                continue

            print(f" Deploying migration: {file.name}")
            cur.execute(file.read_text(encoding="utf-8"))

            cur.execute("INSERT INTO schema_migrations (filename) VALUES (%s)", (file.name,))

    conn.commit()
    conn.close()
    database_name = config.get("database") or config.get("dbname")
    print(f"All '{target}' database structures are live and verified ({database_name}).")


if __name__ == "__main__":
    args = parse_args()
    run_all_migrations(args.target)
