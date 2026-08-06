import datetime
import logging
import numpy as np
import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
np.random.seed(42)

TARGET_ROW_COUNT = 7000

NUMERIC_JITTER_COLS = [
    'age_at_diagnosis', 'nottingham_prognostic_index',
    'overall_survival_months', 'relapse_free_status_months', 'tumor_size'
]
INTEGER_JITTER_COLS = ['mutation_count', 'lymph_nodes_examined_positive', 'tumor_stage']
ALL_JITTER_COLS = NUMERIC_JITTER_COLS + INTEGER_JITTER_COLS
NUMERIC_COERCE_COLS = ALL_JITTER_COLS + ['cohort', 'neoplasm_histologic_grade']

ROUNDING_PRECISION = {
    'nottingham_prognostic_index': 3,
    'overall_survival_months': 2,
    'relapse_free_status_months': 2,
    'age_at_diagnosis': 2,
    'tumor_size': 1,
}


def _apply_rounding(df):
    for col, decimals in ROUNDING_PRECISION.items():
        if col in df.columns:
            df[col] = df[col].round(decimals)
    return df


def extract_bronze_records(src_conn):
    """Reads bronze from the OLTP database (metabric_prod)."""
    with src_conn.cursor() as cur:
        cur.execute("SELECT * FROM staging_metabric ORDER BY patient_id;")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=columns)
    for col in NUMERIC_COERCE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df['diagnosis_date'] = [
        fake.date_between(start_date=datetime.date(2018, 1, 1), end_date=datetime.date(2023, 12, 31))
        for _ in range(len(df))
    ]
    df = _apply_rounding(df)
    src_conn.rollback()  
    logger.info(f"Extracted {len(df)} real records from staging_metabric")
    return df


def _build_capped_synthetic_index(n_real, n_needed, random_state=42):
    base_copies = n_needed // n_real
    remainder = n_needed % n_real
    rng = np.random.RandomState(random_state)
    indices = []
    for _ in range(base_copies):
        indices.extend(rng.permutation(n_real))
    if remainder > 0:
        indices.extend(rng.choice(n_real, size=remainder, replace=False))
    logger.info(f"Capped bootstrap: {base_copies} guaranteed extra copies/row, {remainder} rows get 1 more")
    return indices


def augment_to_target(df, target_rows=TARGET_ROW_COUNT):
    n_needed = target_rows - len(df)
    if n_needed <= 0:
        return df

    local_fake = Faker()
    local_fake.seed_instance(42)

    idx = _build_capped_synthetic_index(len(df), n_needed)
    synthetic = df.iloc[idx].reset_index(drop=True)

    rng = np.random.RandomState(42)

    # Shared multiplicative factor for the two correlated survival fields —
    # applying the SAME factor to both preserves their ratio, so a row that
    # satisfied relapse_free <= overall_survival before jitter still does after
    survival_mask = synthetic['overall_survival_months'].notnull() & synthetic['relapse_free_status_months'].notnull()
    shared_factor = 1 + rng.normal(0, 0.05, size=len(synthetic))
    synthetic.loc[survival_mask, 'overall_survival_months'] = (
        synthetic.loc[survival_mask, 'overall_survival_months'].astype(float) * shared_factor[survival_mask]
    ).clip(lower=0)
    synthetic.loc[survival_mask, 'relapse_free_status_months'] = (
        synthetic.loc[survival_mask, 'relapse_free_status_months'].astype(float) * shared_factor[survival_mask]
    ).clip(lower=0)

    # Remaining columns jitter independently as before — no correlation constraint between them
    other_jitter_cols = [c for c in ALL_JITTER_COLS if c not in ('overall_survival_months', 'relapse_free_status_months')]
    for col in other_jitter_cols:
        std = df[col].std()
        if pd.isna(std) or std == 0:
            continue
        noise = rng.normal(0, std * 0.05, size=len(synthetic))
        mask = synthetic[col].notnull()
        jittered = synthetic.loc[mask, col].astype(float) + noise[mask]
        if col == 'nottingham_prognostic_index':
            synthetic.loc[mask, col] = jittered.clip(lower=1.0, upper=7.5)
        elif col in INTEGER_JITTER_COLS:
            synthetic.loc[mask, col] = jittered.clip(lower=0).round()
        else:
            synthetic.loc[mask, col] = jittered.clip(lower=0)

    synthetic['patient_id'] = [f"SYN-{i:05d}" for i in range(n_needed)]
    synthetic['diagnosis_date'] = [
        local_fake.date_between(start_date=datetime.date(2018, 1, 1), end_date=datetime.date(2023, 12, 31))
        for _ in range(n_needed)
    ]
    synthetic = _apply_rounding(synthetic)

    combined = pd.concat([df, synthetic], ignore_index=True)
    logger.info(f"Augmented {len(df)} real rows to {len(combined)} total ({n_needed} synthetic)")
    return combined


def extract_and_augment(src_conn, target_rows=TARGET_ROW_COUNT):
    real_df = extract_bronze_records(src_conn)
    full_df = augment_to_target(real_df, target_rows)
    return full_df.to_dict(orient='records')


def extract_silver_for_warehouse(src_conn, watermark=None):
    """Joined, fact-ready dataframe — join stays in SQL since it's entirely
    within the OLTP database. The cross-database step happens later, in
    populate_warehouse(), via a pandas merge against the warehouse's
    dim_subtype keys."""
    sql = """
        SELECT
            p.patient_id, p.cohort, p.diagnosis_date, p.age_at_diagnosis, p.age_group,
            t.tumor_size, t.mutation_count, t.nottingham_prognostic_index,
            t.pam50_subtype, t.integrative_cluster, t.three_gene_subtype, t.receptor_profile,
            o.overall_survival_months, o.relapse_free_status_months AS relapse_free_months,
            CASE WHEN o.overall_survival_status = 'Deceased' THEN 1
                 WHEN o.overall_survival_status = 'Living' THEN 0
                 ELSE NULL END AS is_deceased,
            CASE WHEN o.relapse_free_status = 'Recurred' THEN 1
                 WHEN o.relapse_free_status = 'Not Recurred' THEN 0
                 ELSE NULL END AS is_relapsed
        FROM silver_patients p
        JOIN silver_tumor_pathology t ON p.patient_id = t.patient_id
        JOIN silver_outcomes o ON p.patient_id = o.patient_id
    """
    params = None
    if watermark is not None:
        sql += "\nWHERE p.cohort > %(watermark)s"
        params = {"watermark": watermark}

    sql += "\nORDER BY p.cohort, p.patient_id"
    df = pd.read_sql_query(sql, src_conn, params=params)
    load_type = "incremental" if watermark is not None else "full"
    logger.info(f"Extracted {len(df)} {load_type} silver record(s) for warehouse load")
    return df


def extract_silver_for_warehouse_v2(src_conn, watermark=None):
    """Return the complete Silver clinical record required by the V2 Gold star schema."""
    sql = """
        SELECT
            p.patient_id, p.cohort, p.sex, p.inferred_menopausal_state,
            p.age_at_diagnosis,
            t.cancer_type, t.cancer_type_detailed, t.oncotree_code, t.cellularity,
            t.neoplasm_histologic_grade, t.tumor_stage, t.primary_tumor_laterality,
            t.tumor_other_histologic_subtype, t.tumor_size, t.mutation_count,
            t.lymph_nodes_examined_positive, t.nottingham_prognostic_index,
            t.pam50_subtype, t.three_gene_subtype, t.integrative_cluster,
            t.er_status_ihc, t.er_status, t.pr_status, t.her2_status_snp6,
            t.her2_status,
            tr.type_of_breast_surgery, tr.chemotherapy, tr.hormone_therapy,
            tr.radio_therapy,
            o.overall_survival_status, o.overall_survival_months,
            o.relapse_free_status, o.relapse_free_status_months AS relapse_free_months,
            o.vital_status,
            CASE WHEN o.overall_survival_status = 'Deceased' THEN 1
                 WHEN o.overall_survival_status = 'Living' THEN 0
                 ELSE NULL END AS is_deceased,
            CASE WHEN o.relapse_free_status = 'Recurred' THEN 1
                 WHEN o.relapse_free_status = 'Not Recurred' THEN 0
                 ELSE NULL END AS is_relapsed
        FROM silver_patients p
        JOIN silver_tumor_pathology t ON t.patient_id = p.patient_id
        JOIN silver_treatments tr ON tr.patient_id = p.patient_id
        JOIN silver_outcomes o ON o.patient_id = p.patient_id
    """
    params = None
    if watermark is not None:
        sql += "\nWHERE p.cohort > %(watermark)s"
        params = {"watermark": watermark}
    sql += "\nORDER BY p.cohort, p.patient_id"
    with src_conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [column.name for column in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def get_warehouse_watermark(dst_conn):
    """Return the latest cohort already represented in the warehouse fact.

    Cohort is the source's real batch identifier. A zero watermark is only a
    fallback for an empty fact table; callers must still require a full load
    before allowing an incremental run, because source rows with NULL cohort
    are intentionally full-load-only.
    """
    with dst_conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(cohort), 0) FROM fact_patient_outcomes")
        watermark = cur.fetchone()[0]
    logger.info(f"Warehouse cohort watermark: {watermark}")
    return watermark


def extract_subtype_lookup(dst_conn):
    """Pulls current dim_subtype surrogate keys from the warehouse database
    into memory — the cross-database equivalent of rides' extract_lookup_dim()."""
    df = pd.read_sql_query(
        "SELECT subtype_key, pam50_subtype, integrative_cluster, three_gene_subtype FROM dim_subtype",
        dst_conn
    )
    logger.info(f"Loaded {len(df)} subtype lookup rows from warehouse")
    return df
