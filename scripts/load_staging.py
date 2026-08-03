import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def bootstrap_staging_bronze():
    csv_path = './data/staging_metabric.csv'
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing local source data file at {csv_path}")

    conn = psycopg2.connect(
        host=os.getenv("SRC_DB_HOST"),
        database=os.getenv("SRC_DB_NAME"),
        user=os.getenv("SRC_DB_USER"),
        password=os.getenv("SRC_DB_PASSWORD"),
        port=os.getenv("SRC_DB_PORT")
    )

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

        with open(csv_path, 'r') as f:
            cur.copy_expert("COPY staging_metabric FROM STDIN CSV HEADER NULL ''", f)

    conn.commit()
    conn.close()
    print("Bronze Layer Successfully Re-Bootstrapped from local CSV.")

if __name__ == "__main__":
    bootstrap_staging_bronze()