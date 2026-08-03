import argparse
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SRC_DB_CONFIG = dict(
    host=os.getenv("SRC_DB_HOST"),
    database=os.getenv("SRC_DB_NAME"),
    user=os.getenv("SRC_DB_USER"),
    password=os.getenv("SRC_DB_PASSWORD"),
    port=os.getenv("SRC_DB_PORT")
)

DEST_DB_CONFIG = dict(
    host=os.getenv("DEST_DB_HOST"),
    database=os.getenv("DEST_DB_NAME"),
    user=os.getenv("DEST_DB_USER"),
    password=os.getenv("DEST_DB_PASSWORD"),
    port=os.getenv("DEST_DB_PORT")
)


def parse_args():
    parser = argparse.ArgumentParser(description="Run migrations against the OLTP or warehouse database")
    parser.add_argument(
        "--target",
        choices=["oltp", "warehouse"],
        required=True,
        help="Which database to migrate: 'oltp' (metabric_prod) or 'warehouse' (metabric_warehouse)"
    )
    return parser.parse_args()


def run_all_migrations(target: str):
    config = SRC_DB_CONFIG if target == "oltp" else DEST_DB_CONFIG
    migration_dir = f"./migrations/{target}"

    if not os.path.exists(migration_dir):
        raise FileNotFoundError(f"Migration directory not found at {migration_dir}")

    files = sorted([f for f in os.listdir(migration_dir) if f.endswith(".sql")])

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
            cur.execute("SELECT 1 FROM schema_migrations WHERE filename = %s", (file,))
            if cur.fetchone():
                print(f" Skipping already applied migration: {file}")
                continue

            print(f" Deploying migration: {file}")
            with open(os.path.join(migration_dir, file), "r") as f:
                cur.execute(f.read())

            cur.execute("INSERT INTO schema_migrations (filename) VALUES (%s)", (file,))

    conn.commit()
    conn.close()
    print(f"All '{target}' database structures are live and verified ({config['database']}).")


if __name__ == "__main__":
    args = parse_args()
    run_all_migrations(args.target)