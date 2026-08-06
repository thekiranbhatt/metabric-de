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


def truncate_warehouse(dst_conn, commit=True):
    with dst_conn.cursor() as cur:
        cur.execute("TRUNCATE fact_patient_outcomes CASCADE;")
        cur.execute("TRUNCATE dim_date CASCADE;")
        cur.execute("TRUNCATE dim_subtype CASCADE;")
    if commit:
        dst_conn.commit()
    logger.info("Warehouse tables truncated")


def load_dim_date(dst_conn, dates, commit=True):
    rows = []
    for d in set(dates):
        date_key = int(d.strftime("%Y%m%d"))
        rows.append((date_key, d, d.year, d.month, (d.month - 1) // 3 + 1))

    with dst_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dim_date (date_key, full_date, year, month, quarter)
            VALUES %s ON CONFLICT (date_key) DO NOTHING
        """, rows)
    if commit:
        dst_conn.commit()
    logger.info(f"Loaded {len(rows)} dim_date rows")


def load_dim_subtype(dst_conn, subtype_df, commit=True):
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
    if commit:
        dst_conn.commit()
    logger.info(f"Upserted {len(rows)} dim_subtype combinations")


def load_fact_patient_outcomes(dst_conn, fact_df, commit=True):
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
    if commit:
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

    # Establish the watermark before extracting from Silver, then use one
    # destination transaction for every legacy Gold write and reconciliation.
    # The source and warehouse are separate databases, so this is an atomic
    # warehouse publication boundary rather than a distributed transaction.
    dst_conn.rollback()
    if mode == 'full':
        watermark = None
    else:
        _ensure_incremental_baseline(dst_conn)
        watermark = get_warehouse_watermark(dst_conn)
        dst_conn.rollback()

    silver_df = extract_silver_for_warehouse(src_conn, watermark=watermark)
    if silver_df.empty:
        logger.info("No newer cohorts found; warehouse is already up to date")
        return 0

    try:
        if mode == 'full':
            truncate_warehouse(dst_conn, commit=False)

        load_dim_date(dst_conn, silver_df['diagnosis_date'], commit=False)
        subtype_combos = silver_df[['pam50_subtype', 'integrative_cluster', 'three_gene_subtype']].drop_duplicates()
        load_dim_subtype(dst_conn, subtype_combos, commit=False)

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
            raise RuntimeError(f"{unmatched.sum()} row(s) failed to match dim_subtype")
        fact_df['date_key'] = fact_df['diagnosis_date'].apply(lambda d: int(d.strftime("%Y%m%d")))
        inserted_count = load_fact_patient_outcomes(dst_conn, fact_df, commit=False)

        with dst_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_patient_outcomes;")
            fact_count = cur.fetchone()[0]
        if mode == 'full' and fact_count != len(silver_df):
            raise RuntimeError(
                f"Legacy Gold reconciliation failed: expected {len(silver_df)} facts, found {fact_count}"
            )

        dst_conn.commit()
    except Exception:
        dst_conn.rollback()
        logger.exception("Legacy Gold load rolled back; no partial warehouse state was committed")
        raise

    if mode == 'incremental':
        logger.info("Atomic incremental legacy Gold load committed: watermark=%s, inserted=%s", watermark, inserted_count)
    else:
        logger.info("Atomic full legacy Gold load committed: inserted=%s, current_facts=%s", inserted_count, fact_count)
    return inserted_count


V2_DIMENSIONS = {
    'dim_patient_demographics': ['patient_id', 'sex', 'inferred_menopausal_state'],
    'dim_tumor_characteristics': [
        'cancer_type', 'cancer_type_detailed', 'oncotree_code', 'cellularity',
        'neoplasm_histologic_grade', 'tumor_stage', 'primary_tumor_laterality',
        'tumor_other_histologic_subtype',
    ],
    'dim_molecular_subtypes': [
        'pam50_subtype', 'three_gene_subtype', 'integrative_cluster', 'er_status_ihc',
        'er_status', 'pr_status', 'her2_status_snp6', 'her2_status',
    ],
    'dim_treatments': [
        'type_of_breast_surgery', 'chemotherapy', 'hormone_therapy', 'radio_therapy',
    ],
}

V2_DIMENSION_KEYS = {
    'dim_patient_demographics': 'patient_dim_key',
    'dim_tumor_characteristics': 'tumor_characteristic_key',
    'dim_molecular_subtypes': 'molecular_subtype_key',
    'dim_treatments': 'treatment_key',
}

V2_DIMENSION_CONFLICT_COLUMNS = {
    'dim_patient_demographics': ['patient_id'],
    'dim_tumor_characteristics': V2_DIMENSIONS['dim_tumor_characteristics'],
    'dim_molecular_subtypes': V2_DIMENSIONS['dim_molecular_subtypes'],
    'dim_treatments': V2_DIMENSIONS['dim_treatments'],
}


def _normalise_v2_dimensions(df):
    """Normalise nullable categorical values before matching dimension keys."""
    result = df.copy()
    for columns in V2_DIMENSIONS.values():
        for column in columns:
            if column != 'patient_id':
                result[column] = result[column].astype(object).where(result[column].notna(), 'Unknown')
                result[column] = result[column].astype(str)
    return result


def _upsert_v2_dimension(cursor, table, columns, df):
    unique_values = df[columns].drop_duplicates()
    rows = list(unique_values.itertuples(index=False, name=None))
    if not rows:
        return
    conflict_columns = V2_DIMENSION_CONFLICT_COLUMNS[table]
    execute_values(
        cursor,
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT ({', '.join(conflict_columns)}) DO NOTHING",
        rows,
    )


def _attach_v2_dimension_key(cursor, df, table, columns):
    key = V2_DIMENSION_KEYS[table]
    cursor.execute(f"SELECT {key}, {', '.join(columns)} FROM {table}")
    lookup = pd.DataFrame(cursor.fetchall(), columns=[key, *columns])
    result = df.merge(lookup, on=columns, how='left', validate='many_to_one')
    if result[key].isna().any():
        raise RuntimeError(f"V2 dimension lookup failed for {table}")
    return result


def _get_v2_watermark(cursor):
    cursor.execute("SELECT COALESCE(MAX(source_batch), 0) FROM fact_clinical_outcomes_v2")
    return cursor.fetchone()[0]


def _ensure_v2_incremental_baseline(cursor):
    cursor.execute("""
        SELECT COUNT(*), COUNT(source_batch), COUNT(*) FILTER (WHERE source_batch IS NULL)
        FROM fact_clinical_outcomes_v2
    """)
    fact_count, assigned_count, null_batch_count = cursor.fetchone()
    if fact_count == 0 or assigned_count == 0 or null_batch_count == 0:
        raise RuntimeError(
            "V2 incremental warehouse load requires a prior V2 full load; run --mode full first."
        )


def populate_warehouse_v2(src_conn, dst_conn, mode='full'):
    """Build the V2 Gold schema in one destination transaction.

    The legacy Gold fact is intentionally untouched. A failed V2 full or
    incremental load rolls back dimensions and facts together, so reporting
    never observes a partially rebuilt V2 warehouse.
    """
    from pipeline.extract import extract_silver_for_warehouse_v2

    if mode not in {'full', 'incremental'}:
        raise ValueError("mode must be either 'full' or 'incremental'")

    # The preceding legacy load performs a final SELECT for reconciliation.
    # psycopg2 leaves that read transaction open, so close it before starting
    # the single atomic V2 transaction on this shared connection.
    dst_conn.rollback()
    dst_conn.autocommit = False
    try:
        with dst_conn.cursor() as cursor:
            if mode == 'full':
                watermark = None
                cursor.execute("""
                    TRUNCATE fact_clinical_outcomes_v2, dim_patient_demographics,
                        dim_tumor_characteristics, dim_molecular_subtypes, dim_treatments
                    RESTART IDENTITY CASCADE
                """)
            else:
                _ensure_v2_incremental_baseline(cursor)
                watermark = _get_v2_watermark(cursor)

            clinical_df = extract_silver_for_warehouse_v2(src_conn, watermark=watermark)
            if clinical_df.empty:
                dst_conn.rollback()
                logger.info("No newer source batches found for the V2 warehouse")
                return 0

            clinical_df = _normalise_v2_dimensions(clinical_df)
            for table, columns in V2_DIMENSIONS.items():
                _upsert_v2_dimension(cursor, table, columns, clinical_df)
                clinical_df = _attach_v2_dimension_key(cursor, clinical_df, table, columns)

            clinical_df['record_origin'] = clinical_df['patient_id'].apply(
                lambda patient_id: 'augmented' if str(patient_id).startswith('SYN-') else 'original'
            )
            fact_columns = [
                'patient_dim_key', 'tumor_characteristic_key', 'molecular_subtype_key',
                'treatment_key', 'cohort', 'record_origin', 'age_at_diagnosis', 'tumor_size',
                'mutation_count', 'lymph_nodes_examined_positive', 'nottingham_prognostic_index',
                'overall_survival_months', 'relapse_free_months', 'is_deceased', 'is_relapsed',
                'overall_survival_status', 'relapse_free_status', 'vital_status',
            ]
            insert_df = clinical_df[fact_columns].astype(object).where(
                pd.notnull(clinical_df[fact_columns]), None
            )
            rows = list(insert_df.itertuples(index=False, name=None))
            inserted = execute_values(cursor, """
                INSERT INTO fact_clinical_outcomes_v2 (
                    patient_dim_key, tumor_characteristic_key, molecular_subtype_key,
                    treatment_key, source_batch, record_origin, age_at_diagnosis, tumor_size,
                    mutation_count, lymph_nodes_examined_positive, nottingham_prognostic_index,
                    overall_survival_months, relapse_free_months, is_deceased, is_relapsed,
                    overall_survival_status, relapse_free_status, vital_status
                ) VALUES %s
                ON CONFLICT (patient_dim_key) DO NOTHING
                RETURNING clinical_outcome_fact_key
            """, rows, fetch=True)

            cursor.execute("SELECT COUNT(*) FROM fact_clinical_outcomes_v2")
            fact_count = cursor.fetchone()[0]
            if mode == 'full' and fact_count != len(clinical_df):
                raise RuntimeError(
                    f"V2 reconciliation failed: expected {len(clinical_df)} facts, found {fact_count}"
                )

        dst_conn.commit()
        logger.info(
            "V2 Gold %s load committed atomically: inserted=%s, current_facts=%s",
            mode, len(inserted), fact_count,
        )
        return len(inserted)
    except Exception:
        dst_conn.rollback()
        logger.exception("V2 Gold load rolled back; no partial V2 warehouse state was committed")
        raise
