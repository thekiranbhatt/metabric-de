"""Governed database queries for the METABRIC clinical dashboard.

All clinical aggregates use Gold V2 and ``record_origin = 'original'``. Silver
is deliberately limited to an individual patient's row-level drill-down.
"""

from __future__ import annotations

import pandas as pd
import psycopg2
import streamlit as st

try:  # Supports both `streamlit run app/main.py` and package-style imports.
    from app.db import clear_connection_cache, get_oltp_connection, get_warehouse_connection
except ModuleNotFoundError:  # pragma: no cover - used by Streamlit script execution
    from db import clear_connection_cache, get_oltp_connection, get_warehouse_connection


ClinicalFilters = tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...]]
EMPTY_FILTERS: ClinicalFilters = ((), (), (), ())


def _read_warehouse(sql: str, params: dict | None = None) -> pd.DataFrame:
    return _read_with_reconnect(get_warehouse_connection, sql, params)


def _read_oltp(sql: str, params: dict | None = None) -> pd.DataFrame:
    return _read_with_reconnect(get_oltp_connection, sql, params)


def _read_with_reconnect(connection_factory, sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        return _read_dataframe(connection_factory(), sql, params)
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        clear_connection_cache()
        return _read_dataframe(connection_factory(), sql, params)


def _read_dataframe(connection, sql: str, params: dict | None = None) -> pd.DataFrame:
    """Fetch a query and release the read transaction to avoid Airflow locks."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [column.name for column in cursor.description]
            rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        if not connection.closed:
            connection.rollback()


def _clinical_cte(filters: ClinicalFilters = EMPTY_FILTERS) -> tuple[str, dict]:
    """Return the governed clinical base relation and optional chart filters."""
    pam50, age_groups, grades, chemotherapy = filters
    clauses = ["f.record_origin = 'original'"]
    params: dict[str, list[str]] = {}
    if pam50:
        clauses.append("COALESCE(NULLIF(m.pam50_subtype, 'Unknown'), 'Unrecorded') = ANY(%(pam50)s)")
        params["pam50"] = list(pam50)
    if age_groups:
        clauses.append("""CASE
            WHEN f.age_at_diagnosis IS NULL THEN 'Unrecorded'
            WHEN f.age_at_diagnosis < 40 THEN '<40'
            WHEN f.age_at_diagnosis < 50 THEN '40–49'
            WHEN f.age_at_diagnosis < 60 THEN '50–59'
            WHEN f.age_at_diagnosis < 70 THEN '60–69'
            ELSE '70+'
        END = ANY(%(age_groups)s)""")
        params["age_groups"] = list(age_groups)
    if grades:
        clauses.append("COALESCE(NULLIF(t.neoplasm_histologic_grade, 'Unknown'), 'Unrecorded') = ANY(%(grades)s)")
        params["grades"] = list(grades)
    if chemotherapy:
        clauses.append("COALESCE(NULLIF(tr.chemotherapy, 'Unknown'), 'Unrecorded') = ANY(%(chemotherapy)s)")
        params["chemotherapy"] = list(chemotherapy)

    return f"""
        WITH clinical AS (
            SELECT
                f.*, p.patient_id,
                COALESCE(NULLIF(m.pam50_subtype, 'Unknown'), 'Unrecorded') AS pam50_subtype,
                COALESCE(NULLIF(t.neoplasm_histologic_grade, 'Unknown'), 'Unrecorded') AS histologic_grade,
                COALESCE(NULLIF(t.cancer_type, 'Unknown'), 'Unrecorded') AS cancer_type,
                COALESCE(NULLIF(t.cancer_type_detailed, 'Unknown'), 'Unrecorded') AS cancer_type_detailed,
                COALESCE(NULLIF(tr.type_of_breast_surgery, 'Unknown'), 'Unrecorded') AS surgery_type,
                COALESCE(NULLIF(tr.chemotherapy, 'Unknown'), 'Unrecorded') AS chemotherapy,
                COALESCE(NULLIF(tr.hormone_therapy, 'Unknown'), 'Unrecorded') AS hormone_therapy,
                COALESCE(NULLIF(tr.radio_therapy, 'Unknown'), 'Unrecorded') AS radio_therapy,
                CASE
                    WHEN f.age_at_diagnosis IS NULL THEN 'Unrecorded'
                    WHEN f.age_at_diagnosis < 40 THEN '<40'
                    WHEN f.age_at_diagnosis < 50 THEN '40–49'
                    WHEN f.age_at_diagnosis < 60 THEN '50–59'
                    WHEN f.age_at_diagnosis < 70 THEN '60–69'
                    ELSE '70+'
                END AS age_group,
                CASE
                    WHEN m.er_status = 'Negative' AND m.pr_status = 'Negative' AND m.her2_status = 'Negative'
                        THEN 'Triple negative'
                    WHEN m.her2_status = 'Positive' AND (m.er_status = 'Positive' OR m.pr_status = 'Positive')
                        THEN 'HR+ / HER2+'
                    WHEN m.her2_status = 'Positive' THEN 'HER2+'
                    WHEN m.her2_status = 'Negative' AND (m.er_status = 'Positive' OR m.pr_status = 'Positive')
                        THEN 'HR+ / HER2−'
                    ELSE 'Unrecorded'
                END AS receptor_profile
            FROM fact_clinical_outcomes_v2 f
            JOIN dim_patient_demographics p ON p.patient_dim_key = f.patient_dim_key
            JOIN dim_molecular_subtypes m ON m.molecular_subtype_key = f.molecular_subtype_key
            JOIN dim_tumor_characteristics t ON t.tumor_characteristic_key = f.tumor_characteristic_key
            JOIN dim_treatments tr ON tr.treatment_key = f.treatment_key
            WHERE {' AND '.join(clauses)}
        )
    """, params


@st.cache_data(ttl=300, show_spinner=False)
def get_chart_filter_options() -> dict[str, list[str]]:
    cte, params = _clinical_cte()
    options = {}
    for field in ("pam50_subtype", "age_group", "histologic_grade", "chemotherapy"):
        frame = _read_warehouse(f"{cte} SELECT DISTINCT {field} AS value FROM clinical ORDER BY value", params)
        options[field] = frame["value"].tolist()
    return options


@st.cache_data(ttl=300, show_spinner=False)
def get_study_overview() -> pd.DataFrame:
    cte, params = _clinical_cte()
    return _read_warehouse(f"""{cte}
        SELECT COUNT(*) AS original_patients,
               COUNT(*) FILTER (WHERE is_relapsed IS NOT NULL) AS known_relapse_status,
               COUNT(*) FILTER (WHERE is_deceased IS NOT NULL) AS known_survival_status,
               COUNT(*) FILTER (WHERE overall_survival_months IS NOT NULL) AS known_survival_duration,
               COUNT(*) FILTER (WHERE relapse_free_months IS NOT NULL) AS known_relapse_duration
        FROM clinical
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_outcome_kpis() -> pd.DataFrame:
    cte, params = _clinical_cte()
    return _read_warehouse(f"""{cte}
        SELECT COUNT(*) FILTER (WHERE is_relapsed IS NOT NULL) AS relapse_known,
               COUNT(*) FILTER (WHERE is_relapsed = 1) AS relapsed_count,
               COUNT(*) FILTER (WHERE is_deceased IS NOT NULL) AS survival_status_known,
               COUNT(*) FILTER (WHERE is_deceased = 1) AS deceased_count,
               COUNT(*) FILTER (WHERE overall_survival_months IS NOT NULL) AS survival_duration_known,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY overall_survival_months)
                   FILTER (WHERE overall_survival_months IS NOT NULL) AS median_survival_months,
               COUNT(*) FILTER (WHERE nottingham_prognostic_index IS NOT NULL) AS npi_known,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY nottingham_prognostic_index)
                   FILTER (WHERE nottingham_prognostic_index IS NOT NULL) AS median_npi
        FROM clinical
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_endpoint_coverage(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"""{cte}
        SELECT 'Relapse status' AS measure, COUNT(*) FILTER (WHERE is_relapsed IS NOT NULL) AS known, COUNT(*) AS total FROM clinical
        UNION ALL SELECT 'Relapse duration', COUNT(*) FILTER (WHERE relapse_free_months IS NOT NULL), COUNT(*) FROM clinical
        UNION ALL SELECT 'Survival status', COUNT(*) FILTER (WHERE is_deceased IS NOT NULL), COUNT(*) FROM clinical
        UNION ALL SELECT 'Survival duration', COUNT(*) FILTER (WHERE overall_survival_months IS NOT NULL), COUNT(*) FROM clinical
        UNION ALL SELECT 'NPI', COUNT(*) FILTER (WHERE nottingham_prognostic_index IS NOT NULL), COUNT(*) FROM clinical
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_subtype_distribution(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"{cte} SELECT pam50_subtype, COUNT(*) AS patient_count FROM clinical GROUP BY 1 ORDER BY 2 DESC, 1", params)


@st.cache_data(ttl=300, show_spinner=False)
def get_subtype_outcomes(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"""{cte}
        SELECT pam50_subtype, COUNT(*) AS original_patients,
               COUNT(*) FILTER (WHERE is_relapsed IS NOT NULL) AS relapse_known,
               COUNT(*) FILTER (WHERE is_relapsed = 1) AS relapsed_count,
               COUNT(*) FILTER (WHERE is_deceased IS NOT NULL) AS survival_status_known,
               COUNT(*) FILTER (WHERE is_deceased = 1) AS deceased_count,
               COUNT(*) FILTER (WHERE overall_survival_months IS NOT NULL) AS survival_duration_known,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY overall_survival_months)
                   FILTER (WHERE overall_survival_months IS NOT NULL) AS median_survival_months,
               COUNT(*) FILTER (WHERE nottingham_prognostic_index IS NOT NULL) AS npi_known,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY nottingham_prognostic_index)
                   FILTER (WHERE nottingham_prognostic_index IS NOT NULL) AS median_npi
        FROM clinical WHERE pam50_subtype <> 'Unrecorded'
        GROUP BY 1 ORDER BY 2 DESC, 1
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_survival_by_subtype(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"""{cte}
        SELECT pam50_subtype, overall_survival_months
        FROM clinical
        WHERE pam50_subtype <> 'Unrecorded' AND overall_survival_months IS NOT NULL
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_outcome_status_distribution(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"""{cte}
        SELECT CASE WHEN is_deceased = 1 THEN 'Deceased' ELSE 'Living' END AS survival_status, COUNT(*) AS patient_count
        FROM clinical WHERE is_deceased IS NOT NULL GROUP BY 1 ORDER BY 1 DESC
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_receptor_profile_distribution(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"{cte} SELECT receptor_profile, COUNT(*) AS patient_count FROM clinical GROUP BY 1 ORDER BY 2 DESC, 1", params)


@st.cache_data(ttl=300, show_spinner=False)
def get_age_distribution(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"""{cte}
        SELECT age_group, COUNT(*) AS patient_count FROM clinical GROUP BY 1
        ORDER BY CASE age_group WHEN '<40' THEN 1 WHEN '40–49' THEN 2 WHEN '50–59' THEN 3 WHEN '60–69' THEN 4 WHEN '70+' THEN 5 ELSE 6 END
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_age_outcomes(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"""{cte}
        SELECT age_group, COUNT(*) AS original_patients,
               COUNT(*) FILTER (WHERE is_relapsed IS NOT NULL) AS relapse_known,
               COUNT(*) FILTER (WHERE is_relapsed = 1) AS relapsed_count,
               COUNT(*) FILTER (WHERE is_deceased IS NOT NULL) AS survival_known,
               COUNT(*) FILTER (WHERE is_deceased = 1) AS deceased_count
        FROM clinical GROUP BY 1
        ORDER BY CASE age_group WHEN '<40' THEN 1 WHEN '40–49' THEN 2 WHEN '50–59' THEN 3 WHEN '60–69' THEN 4 WHEN '70+' THEN 5 ELSE 6 END
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_surgery_distribution(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"{cte} SELECT surgery_type, COUNT(*) AS patient_count FROM clinical GROUP BY 1 ORDER BY 2 DESC, 1", params)


@st.cache_data(ttl=300, show_spinner=False)
def get_treatment_combination_distribution(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"""{cte}
        SELECT CONCAT(surgery_type, ' · chemo ', chemotherapy, ' · hormone ', hormone_therapy, ' · radio ', radio_therapy) AS treatment_combination,
               COUNT(*) AS patient_count
        FROM clinical GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 10
    """, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_cancer_type_distribution(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"{cte} SELECT cancer_type, COUNT(*) AS patient_count FROM clinical GROUP BY 1 ORDER BY 2 DESC, 1", params)


@st.cache_data(ttl=300, show_spinner=False)
def get_cancer_type_detailed_distribution(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"{cte} SELECT cancer_type_detailed, COUNT(*) AS patient_count FROM clinical GROUP BY 1 ORDER BY 2 DESC, 1", params)


@st.cache_data(ttl=300, show_spinner=False)
def get_npi_values(filters: ClinicalFilters = EMPTY_FILTERS) -> pd.DataFrame:
    cte, params = _clinical_cte(filters)
    return _read_warehouse(f"{cte} SELECT nottingham_prognostic_index FROM clinical WHERE nottingham_prognostic_index IS NOT NULL", params)


@st.cache_data(ttl=300, show_spinner=False)
def get_warehouse_provenance() -> tuple[pd.DataFrame, pd.DataFrame]:
    summary = _read_warehouse("SELECT COUNT(*) AS fact_count, MAX(source_batch) AS max_loaded_cohort FROM fact_clinical_outcomes_v2")
    cohorts = _read_warehouse("""
        SELECT COALESCE(source_batch::TEXT, 'Unassigned') AS cohort, COUNT(*) AS record_count
        FROM fact_clinical_outcomes_v2 GROUP BY cohort ORDER BY MIN(source_batch) NULLS FIRST
    """)
    return summary, cohorts


@st.cache_data(ttl=300, show_spinner=False)
def get_original_patient_ids() -> list[str]:
    records = _read_oltp("SELECT patient_id FROM silver_patients WHERE patient_id NOT LIKE 'SYN-%' ORDER BY patient_id")
    return records["patient_id"].tolist()


@st.cache_data(ttl=300, show_spinner=False)
def get_patient_detail(patient_id: str) -> pd.DataFrame:
    return _read_oltp("""
        SELECT p.patient_id, p.age_at_diagnosis, p.age_group, p.sex, p.cohort, p.inferred_menopausal_state,
               t.cancer_type, t.cancer_type_detailed, t.tumor_size, t.tumor_stage, t.neoplasm_histologic_grade,
               t.tumor_other_histologic_subtype, t.nottingham_prognostic_index, t.pam50_subtype,
               t.integrative_cluster, t.three_gene_subtype, t.er_status, t.er_status_ihc, t.her2_status,
               t.her2_status_snp6, t.pr_status, t.primary_tumor_laterality, t.receptor_profile,
               tr.type_of_breast_surgery, tr.chemotherapy, tr.radio_therapy, tr.hormone_therapy,
               o.overall_survival_status, o.overall_survival_months, o.relapse_free_status,
               o.relapse_free_status_months, o.vital_status
        FROM silver_patients p
        JOIN silver_tumor_pathology t ON t.patient_id = p.patient_id
        JOIN silver_treatments tr ON tr.patient_id = p.patient_id
        JOIN silver_outcomes o ON o.patient_id = p.patient_id
        WHERE p.patient_id = %(patient_id)s AND p.patient_id NOT LIKE 'SYN-%%'
    """, {"patient_id": patient_id})
