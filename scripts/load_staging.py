import gzip
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _source_config() -> dict[str, str | None]:
    return {
        "host": os.getenv("SRC_DB_HOST"),
        "database": os.getenv("SRC_DB_NAME"),
        "user": os.getenv("SRC_DB_USER"),
        "password": os.getenv("SRC_DB_PASSWORD"),
        "port": os.getenv("SRC_DB_PORT"),
    }


def _staging_path() -> Path:
    candidates = [
        PROJECT_ROOT / "data" / "staging_metabric.csv",
        PROJECT_ROOT / "data" / "staging_metabric.csv.gz",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Missing METABRIC source data. Expected data/staging_metabric.csv or its compressed .csv.gz seed."
    )


def bootstrap_staging_bronze(config: dict | None = None, csv_path: str | Path | None = None):
    source_path = Path(csv_path) if csv_path else _staging_path()
    if not source_path.exists():
        raise FileNotFoundError(f"Missing local source data file at {source_path}")

    conn = psycopg2.connect(**(config or _source_config()))

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging_metabric (
                patient_id VARCHAR, age_at_diagnosis NUMERIC, type_of_breast_surgery VARCHAR,
                cancer_type VARCHAR, cancer_type_detailed VARCHAR, cellularity VARCHAR,
                chemotherapy VARCHAR, pam50_claudin_low_subtype VARCHAR, cohort NUMERIC,
                er_status_ihc VARCHAR, er_status VARCHAR, neoplasm_histologic_grade NUMERIC,
                her2_status_snp6 VARCHAR, her2_status VARCHAR, tumor_other_histologic_subtype VARCHAR,
                hormone_therapy VARCHAR, inferred_menopausal_state VARCHAR, integrative_cluster VARCHAR,
                primary_tumor_laterality VARCHAR, lymph_nodes_examined_positive NUMERIC,
                mutation_count NUMERIC, nottingham_prognostic_index NUMERIC, oncotree_code VARCHAR,
                overall_survival_months NUMERIC, overall_survival_status VARCHAR, pr_status VARCHAR,
                radio_therapy VARCHAR, relapse_free_status_months NUMERIC, relapse_free_status VARCHAR,
                sex VARCHAR, three_gene_classifier_subtype VARCHAR, tumor_size NUMERIC,
                tumor_stage NUMERIC, vital_status VARCHAR
            );
        """)

        # Using TRUNCATE to make sure re-runs are safe — without this, running the script twice
        cur.execute("TRUNCATE staging_metabric;")

        opener = gzip.open if source_path.suffix == ".gz" else open
        with opener(source_path, "rt", encoding="utf-8", newline="") as source:
            cur.copy_expert("COPY staging_metabric FROM STDIN CSV HEADER NULL ''", source)

    conn.commit()
    conn.close()
    print("Bronze Layer Successfully Re-Bootstrapped from local CSV.")

if __name__ == "__main__":
    bootstrap_staging_bronze()
