"""Daily METABRIC ETL DAG with explicit full and incremental warehouse paths.

Silver is deliberately rebuilt on every run because it is a deterministic
bootstrap from the fixed METABRIC source. The branch only changes the OLTP to
warehouse sync: full reloads every fact, while incremental appends source
cohorts above the fact-table watermark. No row-level data is passed through
XCom; each task reads or writes its database directly.
"""

import logging
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG, Param, TaskGroup
from airflow.task.trigger_rule import TriggerRule

from pipeline.extract import extract_and_augment
from pipeline.load import load_to_silver, populate_warehouse, populate_warehouse_v2
from pipeline.quality import run_quality_gate
from pipeline.transform import clean_and_transform_record

logger = logging.getLogger(__name__)

SRC_CONN_ID = "metabric_oltp"
DEST_CONN_ID = "metabric_warehouse"


def rebuild_silver():
    """Recreate the deterministic Silver layer from the METABRIC bootstrap."""
    src_conn = PostgresHook(SRC_CONN_ID).get_conn()
    try:
        raw_records = extract_and_augment(src_conn)
        transformed = [clean_and_transform_record(record) for record in raw_records]
        clean, rejected = run_quality_gate(transformed)
        if rejected:
            logger.warning("Quality gate rejected %s record(s)", len(rejected))
        load_to_silver(src_conn, clean)
        logger.info("Silver refresh complete: %s record(s) loaded", len(clean))
    finally:
        src_conn.close()


def choose_load_mode(**context):
    """Select the same full/incremental path for both transitional Gold models."""
    mode = context["params"]["load_mode"]
    suffix = "full" if mode == "full" else "incremental"
    return [
        f"warehouse_sync.{suffix}_legacy_gold_load",
        f"warehouse_sync.{suffix}_gold_v2_load",
    ]


def populate_legacy_gold(mode):
    """Atomically sync the transitional legacy Gold model."""
    src_conn = PostgresHook(SRC_CONN_ID).get_conn()
    dst_conn = PostgresHook(DEST_CONN_ID).get_conn()
    try:
        inserted_count = populate_warehouse(src_conn, dst_conn, mode=mode)
        logger.info("%s legacy Gold load inserted %s fact record(s)", mode, inserted_count)
        return inserted_count
    finally:
        src_conn.close()
        dst_conn.close()


def populate_gold_v2(mode):
    """Atomically sync the canonical Gold V2 clinical model."""
    src_conn = PostgresHook(SRC_CONN_ID).get_conn()
    dst_conn = PostgresHook(DEST_CONN_ID).get_conn()
    try:
        inserted_count = populate_warehouse_v2(src_conn, dst_conn, mode=mode)
        logger.info("%s Gold V2 load inserted %s fact record(s)", mode, inserted_count)
        return inserted_count
    finally:
        src_conn.close()
        dst_conn.close()


def validate_warehouse_load():
    """Fail when either Gold model is missing essential post-load structures."""
    dst_conn = PostgresHook(DEST_CONN_ID).get_conn()
    try:
        with dst_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM fact_patient_outcomes),
                    (SELECT MAX(cohort) FROM fact_patient_outcomes),
                    (SELECT COUNT(cohort) FROM fact_patient_outcomes),
                    (SELECT COUNT(*) FROM dim_date),
                    (SELECT COUNT(*) FROM dim_subtype),
                    (SELECT COUNT(*) FROM fact_clinical_outcomes_v2),
                    (SELECT MAX(source_batch) FROM fact_clinical_outcomes_v2),
                    (SELECT COUNT(source_batch) FROM fact_clinical_outcomes_v2),
                    (SELECT COUNT(*) FROM dim_patient_demographics),
                    (SELECT COUNT(*) FROM dim_tumor_characteristics),
                    (SELECT COUNT(*) FROM dim_molecular_subtypes),
                    (SELECT COUNT(*) FROM dim_treatments)
            """)
            (
                legacy_facts, legacy_watermark, legacy_assigned, date_count, subtype_count,
                v2_facts, v2_watermark, v2_assigned, v2_patient_dim, v2_tumor_dim,
                v2_molecular_dim, v2_treatment_dim,
            ) = cur.fetchone()

        failures = []
        if legacy_facts == 0:
            failures.append("fact_patient_outcomes is empty")
        if legacy_watermark is None or legacy_assigned == 0:
            failures.append("legacy Gold has no cohort watermark")
        if date_count == 0 or subtype_count == 0:
            failures.append("one or more legacy Gold dimensions are empty")
        if v2_facts == 0:
            failures.append("fact_clinical_outcomes_v2 is empty")
        if v2_watermark is None or v2_assigned == 0:
            failures.append("Gold V2 has no source-batch watermark")
        if min(v2_patient_dim, v2_tumor_dim, v2_molecular_dim, v2_treatment_dim) == 0:
            failures.append("one or more Gold V2 dimensions are empty")
        if failures:
            raise RuntimeError("Warehouse validation failed: " + "; ".join(failures))

        logger.info(
            "Dual-Gold validation passed: legacy_facts=%s, legacy_watermark=%s, "
            "v2_facts=%s, v2_watermark=%s",
            legacy_facts, legacy_watermark, v2_facts, v2_watermark,
        )
    finally:
        dst_conn.close()


def reconcile_gold_models():
    """Verify the transitional legacy and canonical V2 facts agree on coverage."""
    src_conn = PostgresHook(SRC_CONN_ID).get_conn()
    dst_conn = PostgresHook(DEST_CONN_ID).get_conn()
    try:
        with dst_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM fact_patient_outcomes),
                    (SELECT COUNT(*) FROM fact_clinical_outcomes_v2),
                    (SELECT MAX(cohort) FROM fact_patient_outcomes),
                    (SELECT MAX(source_batch) FROM fact_clinical_outcomes_v2),
                    (SELECT COUNT(*) FROM fact_clinical_outcomes_v2 WHERE record_origin = 'original')
            """)
            legacy_facts, v2_facts, legacy_watermark, v2_watermark, v2_originals = cur.fetchone()
        with src_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM silver_patients WHERE patient_id NOT LIKE 'SYN-%'")
            silver_originals = cur.fetchone()[0]

        failures = []
        if legacy_facts != v2_facts:
            failures.append(f"fact counts differ: legacy={legacy_facts}, v2={v2_facts}")
        if legacy_watermark != v2_watermark:
            failures.append(f"watermarks differ: legacy={legacy_watermark}, v2={v2_watermark}")
        if v2_originals != silver_originals:
            failures.append(f"original records differ: Silver={silver_originals}, V2={v2_originals}")
        if failures:
            raise RuntimeError("Gold reconciliation failed: " + "; ".join(failures))
        logger.info(
            "Gold reconciliation passed: facts=%s, watermark=%s, original_records=%s",
            v2_facts, v2_watermark, v2_originals,
        )
    finally:
        src_conn.close()
        dst_conn.close()


def warehouse_quality_summary(**context):
    """Report post-load metrics for both Gold models after reconciliation."""
    dst_conn = PostgresHook(DEST_CONN_ID).get_conn()
    try:
        with dst_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM fact_patient_outcomes),
                    (SELECT COUNT(*) FROM fact_clinical_outcomes_v2),
                    (SELECT MAX(cohort) FROM fact_patient_outcomes),
                    (SELECT MAX(source_batch) FROM fact_clinical_outcomes_v2),
                    (SELECT COUNT(*) FROM fact_clinical_outcomes_v2 WHERE record_origin = 'original')
            """)
            legacy_facts, v2_facts, legacy_watermark, v2_watermark, v2_originals = cur.fetchone()
        suffix = "full" if context["params"]["load_mode"] == "full" else "incremental"
        legacy_inserted = context["ti"].xcom_pull(task_ids=f"warehouse_sync.{suffix}_legacy_gold_load")
        v2_inserted = context["ti"].xcom_pull(task_ids=f"warehouse_sync.{suffix}_gold_v2_load")
        logger.info(
            "Dual-Gold summary: legacy_facts=%s, v2_facts=%s, legacy_watermark=%s, "
            "v2_watermark=%s, V2_originals=%s, legacy_inserted=%s, v2_inserted=%s",
            legacy_facts, v2_facts, legacy_watermark, v2_watermark, v2_originals,
            legacy_inserted, v2_inserted,
        )
    finally:
        dst_conn.close()


default_args = {
    "owner": "metabric-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="metabric_oltp_to_warehouse",
    description="Daily METABRIC Silver refresh with transitional legacy and canonical Gold V2 sync",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "load_mode": Param(
            "incremental",
            type="string",
            enum=["full", "incremental"],
            description="Use full for initialization/recovery; incremental requires a prior full load.",
        ),
    },
    tags=["metabric", "etl", "daily", "incremental", "gold-v2", "reconciliation"],
    doc_md="""
    ## METABRIC dual-Gold warehouse refresh

    Silver is always full-refreshed because it is a deterministic rebuild from the
    source dataset. The selected **full** or **incremental** mode is applied to the
    transitional legacy Gold model and canonical Gold V2 model. Both loads must
    succeed before validation and reconciliation publish a successful DAG run.
    """,
) as dag:
    with TaskGroup("silver_refresh", tooltip="Rebuild deterministic Silver tables on every run.") as silver_refresh:
        refresh_silver = PythonOperator(
            task_id="rebuild_silver",
            python_callable=rebuild_silver,
            doc_md="Silver is a deterministic full refresh from the METABRIC source.",
        )

    with TaskGroup("warehouse_sync", tooltip="Apply one selected mode to legacy Gold and canonical Gold V2.") as warehouse_sync:
        select_mode = BranchPythonOperator(
            task_id="choose_load_mode",
            python_callable=choose_load_mode,
            doc_md="One full or incremental mode is applied consistently to both Gold models.",
        )
        full_legacy_load = PythonOperator(
            task_id="full_legacy_gold_load",
            python_callable=populate_legacy_gold,
            op_kwargs={"mode": "full"},
            doc_md="Atomically rebuilds the transitional legacy Gold model.",
        )
        full_v2_load = PythonOperator(
            task_id="full_gold_v2_load",
            python_callable=populate_gold_v2,
            op_kwargs={"mode": "full"},
            doc_md="Atomically rebuilds the canonical Gold V2 clinical model.",
        )
        incremental_legacy_load = PythonOperator(
            task_id="incremental_legacy_gold_load",
            python_callable=populate_legacy_gold,
            op_kwargs={"mode": "incremental"},
            doc_md="Atomically appends legacy Gold cohorts above its watermark.",
        )
        incremental_v2_load = PythonOperator(
            task_id="incremental_gold_v2_load",
            python_callable=populate_gold_v2,
            op_kwargs={"mode": "incremental"},
            doc_md="Atomically appends Gold V2 source batches above its watermark.",
        )
        select_mode >> [full_legacy_load, full_v2_load, incremental_legacy_load, incremental_v2_load]

    with TaskGroup("validation_reporting", tooltip="Validate and reconcile both Gold models before reporting success.") as validation_reporting:
        validate = PythonOperator(
            task_id="validate_warehouse_load",
            python_callable=validate_warehouse_load,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            doc_md="Requires both selected Gold branches to complete without failure and validates their structures.",
        )
        reconcile = PythonOperator(
            task_id="reconcile_gold_models",
            python_callable=reconcile_gold_models,
            doc_md="Fails if legacy and Gold V2 coverage, watermarks, or original-record counts differ.",
        )
        summarize = PythonOperator(
            task_id="warehouse_quality_summary",
            python_callable=warehouse_quality_summary,
            doc_md="Reports warehouse metrics; NULL-cohort count is not a failure condition.",
        )
        validate >> reconcile >> summarize

    silver_refresh >> warehouse_sync
    [full_legacy_load, full_v2_load, incremental_legacy_load, incremental_v2_load] >> validation_reporting
