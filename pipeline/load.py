import pandas as pd 
import logging
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def load_to_silver(src_conn, clean_records):
    """Writes into the OLTP database (metabric_prod)."""
    src_conn.autocommit = False
    try:
        with src_conn.cursor() as cur:            
            cur.execute("""
                TRUNCATE silver_outcomes, silver_treatments, silver_tumor_pathology, silver_patients;
            """)

            for i, r in enumerate(clean_records):
                try:
                    cur.execute("""
                        INSERT INTO silver_patients (patient_id, age_at_diagnosis, sex, cohort, diagnosis_date, inferred_menopausal_state, age_group)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, (r['patient_id'], r['age_at_diagnosis'], r['sex'], r['cohort'], r['diagnosis_date'], r['inferred_menopausal_state'], r['age_group']))

                    cur.execute("""
                        INSERT INTO silver_tumor_pathology (
                            patient_id, cancer_type, cancer_type_detailed, cellularity,
                            neoplasm_histologic_grade, tumor_other_histologic_subtype, tumor_size,
                            tumor_stage, oncotree_code, lymph_nodes_examined_positive, mutation_count,
                            nottingham_prognostic_index, pam50_subtype, integrative_cluster, three_gene_subtype,
                            er_status, er_status_ihc, her2_status, her2_status_snp6, pr_status, primary_tumor_laterality,
                            receptor_profile
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        r['patient_id'], r['cancer_type'], r['cancer_type_detailed'], r['cellularity'],
                        r['neoplasm_histologic_grade'], r['tumor_other_histologic_subtype'], r['tumor_size'],
                        r['tumor_stage'], r['oncotree_code'], r['lymph_nodes_examined_positive'], r['mutation_count'],
                        r['nottingham_prognostic_index'], r['pam50_subtype'], r['integrative_cluster'], r['three_gene_subtype'],
                        r['er_status'], r['er_status_ihc'], r['her2_status'], r['her2_status_snp6'], r['pr_status'], r['primary_tumor_laterality'],
                        r['receptor_profile']
                    ))

                    cur.execute("""
                        INSERT INTO silver_treatments (patient_id, type_of_breast_surgery, chemotherapy, radio_therapy, hormone_therapy)
                        VALUES (%s, %s, %s, %s, %s);
                    """, (r['patient_id'], r['type_of_breast_surgery'], r['chemotherapy'], r['radio_therapy'], r['hormone_therapy']))

                    cur.execute("""
                        INSERT INTO silver_outcomes (patient_id, overall_survival_status, overall_survival_months, relapse_free_status, relapse_free_status_months, vital_status)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (r['patient_id'], r['overall_survival_status'], r['overall_survival_months'], r['relapse_free_status'], r['relapse_free_status_months'], r['vital_status']))

                except Exception as e:
                    logger.error(f"Row {i} (patient_id={r.get('patient_id')}) failed to load: {e}")
                    raise

        src_conn.commit()
        logger.info(f"{len(clean_records)} inserted to silver_patients")
        logger.info(f"{len(clean_records)} inserted to silver_tumor_pathology")
        logger.info(f"{len(clean_records)} inserted to silver_treatments")
        logger.info(f"{len(clean_records)} inserted to silver_outcomes")
    except Exception as e:
        src_conn.rollback()
        logger.error(f"Silver load aborted — full batch rolled back: {e}")
        raise


def truncate_warehouse(dst_conn):
    with dst_conn.cursor() as cur:
        cur.execute("TRUNCATE fact_patient_outcomes CASCADE;")
        cur.execute("TRUNCATE dim_date CASCADE;")
        cur.execute("TRUNCATE dim_subtype CASCADE;")
    dst_conn.commit()
    logger.info("Warehouse tables truncated")


def load_dim_date(dst_conn, dates):
    rows = []
    for d in set(dates):
        date_key = int(d.strftime("%Y%m%d"))
        rows.append((date_key, d, d.year, d.month, (d.month - 1) // 3 + 1))

    with dst_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dim_date (date_key, full_date, year, month, quarter)
            VALUES %s ON CONFLICT (date_key) DO NOTHING
        """, rows)
    dst_conn.commit()
    logger.info(f"Loaded {len(rows)} dim_date rows")


def load_dim_subtype(dst_conn, subtype_df):
    rows = [
        (row['pam50_subtype'] or 'Unknown', row['integrative_cluster'] or 'Unknown', row['three_gene_subtype'] or 'Unknown')
        for _, row in subtype_df.iterrows()
    ]
    with dst_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dim_subtype (pam50_subtype, integrative_cluster, three_gene_subtype)
            VALUES %s
            ON CONFLICT (pam50_subtype, integrative_cluster, three_gene_subtype) DO NOTHING
        """, rows)
    dst_conn.commit()
    logger.info(f"Upserted {len(rows)} dim_subtype combinations")


def load_fact_patient_outcomes(dst_conn, fact_df):
    cols = [
        'patient_id', 'cohort', 'date_key', 'subtype_key', 'age_at_diagnosis', 'tumor_size',
        'mutation_count', 'nottingham_prognostic_index', 'overall_survival_months',
        'relapse_free_months', 'is_deceased', 'is_relapsed', 'age_group', 'receptor_profile'
    ]

    # Replace pandas NaN with None across the board — this is what turns a
    # missing value into a real SQL NULL instead of a float('nan') that
    # Postgres rejects with "integer out of range" on INT-typed columns
    insert_df = fact_df[cols].astype(object).where(pd.notnull(fact_df[cols]), None)

    rows = list(insert_df.itertuples(index=False, name=None))

    if not rows:
        logger.info("No fact rows to load — no newer cohorts found")
        return 0

    with dst_conn.cursor() as cur:
        inserted_rows = execute_values(cur, f"""
            INSERT INTO fact_patient_outcomes ({', '.join(cols)})
            VALUES %s
            ON CONFLICT (patient_id) DO NOTHING
            RETURNING patient_id
        """, rows, fetch=True)
        inserted_count = len(inserted_rows)
    dst_conn.commit()
    logger.info(f"Inserted {inserted_count} of {len(rows)} row(s) into fact_patient_outcomes")
    return inserted_count


def _ensure_incremental_baseline(dst_conn):
    """Ensure the warehouse was initialized by a full METABRIC load.

    A valid baseline contains both cohort-assigned patients and the known
    NULL-cohort patients. The latter are deliberately unavailable to a
    cohort-watermark extract, so their presence proves a full load occurred.
    """
    with dst_conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) AS fact_count,
                COUNT(cohort) AS cohort_assigned_count,
                COUNT(*) FILTER (WHERE cohort IS NULL) AS null_cohort_count
            FROM fact_patient_outcomes
        """)
        fact_count, cohort_assigned_count, null_cohort_count = cur.fetchone()

    if fact_count == 0 or cohort_assigned_count == 0 or null_cohort_count == 0:
        raise RuntimeError(
            "Incremental warehouse load requires a prior full load. "
            "Run --mode full first; NULL-cohort patients are full-load-only."
        )


def populate_warehouse(src_conn, dst_conn, mode='full'):
    """The cross-database transport step. src_conn = metabric_prod (OLTP),
    dst_conn = metabric_warehouse. No SQL join spans the two — everything
    from the merge onward is pandas, mirroring rides' extract_lookup_dim +
    transform_trips pattern, condensed into one function here."""
    from pipeline.extract import (
        extract_silver_for_warehouse,
        extract_subtype_lookup,
        get_warehouse_watermark,
    )

    if mode not in {'full', 'incremental'}:
        raise ValueError("mode must be either 'full' or 'incremental'")

    if mode == 'full':
        truncate_warehouse(dst_conn)
        watermark = None
    else:
        _ensure_incremental_baseline(dst_conn)
        watermark = get_warehouse_watermark(dst_conn)

    silver_df = extract_silver_for_warehouse(src_conn, watermark=watermark)

    if silver_df.empty:
        logger.info("No newer cohorts found; warehouse is already up to date")
        return 0

    load_dim_date(dst_conn, silver_df['diagnosis_date'])

    subtype_combos = silver_df[['pam50_subtype', 'integrative_cluster', 'three_gene_subtype']].drop_duplicates()
    load_dim_subtype(dst_conn, subtype_combos)

    subtype_lookup = extract_subtype_lookup(dst_conn).rename(columns={
        'pam50_subtype': 'pam50_subtype_lk',
        'integrative_cluster': 'integrative_cluster_lk',
        'three_gene_subtype': 'three_gene_subtype_lk'
    })

    fact_df = silver_df.copy()
    fact_df['pam50_subtype_lk'] = fact_df['pam50_subtype'].fillna('Unknown')
    fact_df['integrative_cluster_lk'] = fact_df['integrative_cluster'].fillna('Unknown')
    fact_df['three_gene_subtype_lk'] = fact_df['three_gene_subtype'].fillna('Unknown')

    fact_df = fact_df.merge(
        subtype_lookup,
        on=['pam50_subtype_lk', 'integrative_cluster_lk', 'three_gene_subtype_lk'],
        how='left'
    )

    unmatched = fact_df['subtype_key'].isna()
    if unmatched.any():
        logger.warning(f"{unmatched.sum()} row(s) failed to match dim_subtype — dropped from fact load")
        fact_df = fact_df[~unmatched]

    fact_df['date_key'] = fact_df['diagnosis_date'].apply(lambda d: int(d.strftime("%Y%m%d")))

    inserted_count = load_fact_patient_outcomes(dst_conn, fact_df)

    with dst_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fact_patient_outcomes;")
        fact_count = cur.fetchone()[0]
    silver_count = len(silver_df)
    if fact_count < silver_count:
        logger.warning(f"{silver_count - fact_count} silver patient(s) did not make it into the warehouse fact table")
    else:
        logger.info(f"All {silver_count} silver patients present in warehouse fact table")

    if mode == 'incremental':
        logger.info(
            "Incremental warehouse population complete: watermark=%s, inserted=%s",
            watermark,
            inserted_count,
        )
    else:
        logger.info("Full warehouse population complete: inserted=%s", inserted_count)
    return inserted_count
