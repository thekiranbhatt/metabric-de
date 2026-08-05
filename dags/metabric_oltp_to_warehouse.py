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
from pipeline.load import load_to_silver, populate_warehouse
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
    """Return the one warehouse task that Airflow should execute."""
    mode = context["params"]["load_mode"]
    return (
        "warehouse_sync.full_warehouse_load"
        if mode == "full"
        else "warehouse_sync.incremental_warehouse_load"
    )


def populate_selected_warehouse(mode):
    """Sync the warehouse; ``cohort`` is the incremental watermark."""
    src_conn = PostgresHook(SRC_CONN_ID).get_conn()
    dst_conn = PostgresHook(DEST_CONN_ID).get_conn()
    try:
        inserted_count = populate_warehouse(src_conn, dst_conn, mode=mode)
        logger.info("%s warehouse load inserted %s fact record(s)", mode, inserted_count)
        return inserted_count
    finally:
        src_conn.close()
        dst_conn.close()


def validate_warehouse_load():
    """Fail only when essential warehouse tables or the cohort watermark are absent."""
    dst_conn = PostgresHook(DEST_CONN_ID).get_conn()
    try:
        with dst_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM fact_patient_outcomes) AS fact_count,
                    (SELECT MAX(cohort) FROM fact_patient_outcomes) AS cohort_watermark,
                    (SELECT COUNT(cohort) FROM fact_patient_outcomes) AS cohort_assigned_count,
                    (SELECT COUNT(*) FROM dim_date) AS date_count,
                    (SELECT COUNT(*) FROM dim_subtype) AS subtype_count
            """)
            fact_count, watermark, cohort_assigned_count, date_count, subtype_count = cur.fetchone()

        failures = []
        if fact_count == 0:
            failures.append("fact_patient_outcomes is empty")
        if watermark is None:
            failures.append("MAX(cohort) is NULL")
        if cohort_assigned_count == 0:
            failures.append("no non-NULL cohort rows exist")
        if date_count == 0:
            failures.append("dim_date is empty")
        if subtype_count == 0:
            failures.append("dim_subtype is empty")
        if failures:
            raise RuntimeError("Warehouse validation failed: " + "; ".join(failures))

        logger.info(
            "Warehouse validation passed: facts=%s, watermark=%s, cohort-assigned=%s, "
            "dates=%s, subtypes=%s",
            fact_count,
            watermark,
            cohort_assigned_count,
            date_count,
            subtype_count,
        )
    finally:
        dst_conn.close()


def warehouse_quality_summary(**context):
    """Report post-load metrics; the NULL-cohort count is observational, not a gate."""
    dst_conn = PostgresHook(DEST_CONN_ID).get_conn()
    try:
        with dst_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS fact_count,
                    COUNT(*) FILTER (WHERE cohort IS NULL) AS null_cohort_count,
                    MAX(cohort) AS cohort_watermark,
                    (SELECT COUNT(*) FROM dim_date) AS date_count,
                    (SELECT COUNT(*) FROM dim_subtype) AS subtype_count
                FROM fact_patient_outcomes
            """)
            fact_count, null_cohort_count, watermark, date_count, subtype_count = cur.fetchone()
        selected_load_task = (
            "warehouse_sync.full_warehouse_load"
            if context["params"]["load_mode"] == "full"
            else "warehouse_sync.incremental_warehouse_load"
        )
        inserted_count = context["ti"].xcom_pull(task_ids=selected_load_task)
        logger.info(
            "Warehouse summary: facts=%s, watermark=%s, NULL cohorts=%s, dates=%s, "
            "subtypes=%s, inserted=%s",
            fact_count,
            watermark,
            null_cohort_count,
            date_count,
            subtype_count,
            inserted_count,
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
    description="Daily METABRIC Silver refresh and full/incremental warehouse sync",
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
    tags=["metabric", "etl", "daily", "incremental"],
    doc_md="""
    ## METABRIC warehouse refresh

    Silver is always full-refreshed because it is a deterministic rebuild from the
    source dataset. A warehouse **full** load establishes the baseline; only then
    may an **incremental** load use `cohort` as its watermark. Validation gates the
    DAG on warehouse health, while the quality summary reports operational metrics.
    """,
) as dag:
    with TaskGroup("silver_refresh", tooltip="Rebuild deterministic Silver tables on every run.") as silver_refresh:
        refresh_silver = PythonOperator(
            task_id="rebuild_silver",
            python_callable=rebuild_silver,
            doc_md="Silver is a deterministic full refresh from the METABRIC source.",
        )

    with TaskGroup("warehouse_sync", tooltip="Select one full or incremental warehouse sync path.") as warehouse_sync:
        select_mode = BranchPythonOperator(
            task_id="choose_load_mode",
            python_callable=choose_load_mode,
            doc_md="A full load establishes the baseline required before incremental loads.",
        )
        full_load = PythonOperator(
            task_id="full_warehouse_load",
            python_callable=populate_selected_warehouse,
            op_kwargs={"mode": "full"},
            doc_md="Rebuilds warehouse facts and dimensions for initialization or recovery.",
        )
        incremental_load = PythonOperator(
            task_id="incremental_warehouse_load",
            python_callable=populate_selected_warehouse,
            op_kwargs={"mode": "incremental"},
            doc_md="Appends cohorts above the fact-table `cohort` watermark after a full baseline.",
        )
        select_mode >> [full_load, incremental_load]

    with TaskGroup("validation_reporting", tooltip="Validate warehouse invariants and log operational metrics.") as validation_reporting:
        validate = PythonOperator(
            task_id="validate_warehouse_load",
            python_callable=validate_warehouse_load,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            doc_md="Fails only for missing facts, watermark, cohort-assigned rows, or dimensions.",
        )
        summarize = PythonOperator(
            task_id="warehouse_quality_summary",
            python_callable=warehouse_quality_summary,
            doc_md="Reports warehouse metrics; NULL-cohort count is not a failure condition.",
        )
        validate >> summarize

    silver_refresh >> warehouse_sync
    [full_load, incremental_load] >> validation_reporting
