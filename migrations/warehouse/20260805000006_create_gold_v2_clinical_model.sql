-- Gold V2 is a parallel clinical star schema.  The legacy Gold objects remain
-- available during reconciliation and dashboard cutover.

CREATE TABLE IF NOT EXISTS dim_patient_demographics (
    patient_dim_key            SERIAL PRIMARY KEY,
    patient_id                 VARCHAR NOT NULL UNIQUE,
    sex                        VARCHAR NOT NULL DEFAULT 'Unknown',
    inferred_menopausal_state  VARCHAR NOT NULL DEFAULT 'Unknown'
);

CREATE TABLE IF NOT EXISTS dim_tumor_characteristics (
    tumor_characteristic_key       SERIAL PRIMARY KEY,
    cancer_type                    VARCHAR NOT NULL DEFAULT 'Unknown',
    cancer_type_detailed           VARCHAR NOT NULL DEFAULT 'Unknown',
    oncotree_code                  VARCHAR NOT NULL DEFAULT 'Unknown',
    cellularity                    VARCHAR NOT NULL DEFAULT 'Unknown',
    neoplasm_histologic_grade      VARCHAR NOT NULL DEFAULT 'Unknown',
    tumor_stage                    VARCHAR NOT NULL DEFAULT 'Unknown',
    primary_tumor_laterality       VARCHAR NOT NULL DEFAULT 'Unknown',
    tumor_other_histologic_subtype VARCHAR NOT NULL DEFAULT 'Unknown',
    CONSTRAINT uq_dim_tumor_characteristics UNIQUE (
        cancer_type, cancer_type_detailed, oncotree_code, cellularity,
        neoplasm_histologic_grade, tumor_stage, primary_tumor_laterality,
        tumor_other_histologic_subtype
    )
);

CREATE TABLE IF NOT EXISTS dim_molecular_subtypes (
    molecular_subtype_key  SERIAL PRIMARY KEY,
    pam50_subtype          VARCHAR NOT NULL DEFAULT 'Unknown',
    three_gene_subtype     VARCHAR NOT NULL DEFAULT 'Unknown',
    integrative_cluster    VARCHAR NOT NULL DEFAULT 'Unknown',
    er_status_ihc          VARCHAR NOT NULL DEFAULT 'Unknown',
    er_status              VARCHAR NOT NULL DEFAULT 'Unknown',
    pr_status              VARCHAR NOT NULL DEFAULT 'Unknown',
    her2_status_snp6       VARCHAR NOT NULL DEFAULT 'Unknown',
    her2_status            VARCHAR NOT NULL DEFAULT 'Unknown',
    CONSTRAINT uq_dim_molecular_subtypes UNIQUE (
        pam50_subtype, three_gene_subtype, integrative_cluster,
        er_status_ihc, er_status, pr_status, her2_status_snp6, her2_status
    )
);

CREATE TABLE IF NOT EXISTS dim_treatments (
    treatment_key           SERIAL PRIMARY KEY,
    type_of_breast_surgery  VARCHAR NOT NULL DEFAULT 'Unknown',
    chemotherapy            VARCHAR NOT NULL DEFAULT 'Unknown',
    hormone_therapy         VARCHAR NOT NULL DEFAULT 'Unknown',
    radio_therapy           VARCHAR NOT NULL DEFAULT 'Unknown',
    CONSTRAINT uq_dim_treatments UNIQUE (
        type_of_breast_surgery, chemotherapy, hormone_therapy, radio_therapy
    )
);

CREATE TABLE IF NOT EXISTS fact_clinical_outcomes_v2 (
    clinical_outcome_fact_key   SERIAL PRIMARY KEY,
    patient_dim_key             INT NOT NULL UNIQUE REFERENCES dim_patient_demographics(patient_dim_key),
    tumor_characteristic_key    INT NOT NULL REFERENCES dim_tumor_characteristics(tumor_characteristic_key),
    molecular_subtype_key       INT NOT NULL REFERENCES dim_molecular_subtypes(molecular_subtype_key),
    treatment_key               INT NOT NULL REFERENCES dim_treatments(treatment_key),
    source_batch                INTEGER,
    record_origin               VARCHAR NOT NULL,
    age_at_diagnosis            NUMERIC,
    tumor_size                  NUMERIC,
    mutation_count              NUMERIC,
    lymph_nodes_examined_positive NUMERIC,
    nottingham_prognostic_index NUMERIC,
    overall_survival_months     NUMERIC,
    relapse_free_months         NUMERIC,
    is_deceased                 SMALLINT,
    is_relapsed                 SMALLINT,
    overall_survival_status     VARCHAR,
    relapse_free_status         VARCHAR,
    vital_status                VARCHAR,
    CONSTRAINT chk_v2_record_origin CHECK (record_origin IN ('original', 'augmented')),
    CONSTRAINT chk_v2_is_deceased CHECK (is_deceased IS NULL OR is_deceased IN (0, 1)),
    CONSTRAINT chk_v2_is_relapsed CHECK (is_relapsed IS NULL OR is_relapsed IN (0, 1)),
    CONSTRAINT chk_v2_relapse_months_nonnegative CHECK (
        relapse_free_months IS NULL OR relapse_free_months >= 0
    ),
    CONSTRAINT chk_v2_relapse_not_after_survival CHECK (
        relapse_free_months IS NULL OR overall_survival_months IS NULL
        OR relapse_free_months <= overall_survival_months
    )
);

CREATE INDEX IF NOT EXISTS idx_fact_clinical_outcomes_v2_source_batch
    ON fact_clinical_outcomes_v2 (source_batch);
CREATE INDEX IF NOT EXISTS idx_fact_clinical_outcomes_v2_origin
    ON fact_clinical_outcomes_v2 (record_origin);

-- One row per eligible patient and endpoint.  An event_observed value of 0
-- represents a censored observation; this view deliberately includes origin
-- and coverage fields so a clinical consumer can exclude augmented records.
CREATE OR REPLACE VIEW view_kaplan_meier_survival AS
SELECT
    p.patient_id,
    f.record_origin,
    'overall_survival'::VARCHAR AS endpoint,
    f.overall_survival_months AS duration_months,
    f.is_deceased AS event_observed,
    m.pam50_subtype,
    f.age_at_diagnosis,
    f.nottingham_prognostic_index
FROM fact_clinical_outcomes_v2 f
JOIN dim_patient_demographics p ON p.patient_dim_key = f.patient_dim_key
JOIN dim_molecular_subtypes m ON m.molecular_subtype_key = f.molecular_subtype_key
WHERE f.overall_survival_months IS NOT NULL
  AND f.is_deceased IS NOT NULL
UNION ALL
SELECT
    p.patient_id,
    f.record_origin,
    'relapse_free_survival'::VARCHAR AS endpoint,
    f.relapse_free_months AS duration_months,
    f.is_relapsed AS event_observed,
    m.pam50_subtype,
    f.age_at_diagnosis,
    f.nottingham_prognostic_index
FROM fact_clinical_outcomes_v2 f
JOIN dim_patient_demographics p ON p.patient_dim_key = f.patient_dim_key
JOIN dim_molecular_subtypes m ON m.molecular_subtype_key = f.molecular_subtype_key
WHERE f.relapse_free_months IS NOT NULL
  AND f.is_relapsed IS NOT NULL;

-- Descriptive associations only: treatment timing, indication, and regimen are
-- unavailable, so this view must not be interpreted as treatment efficacy.
CREATE OR REPLACE VIEW agg_subtype_treatment_outcomes AS
SELECT
    m.pam50_subtype,
    t.type_of_breast_surgery,
    t.chemotherapy,
    t.hormone_therapy,
    t.radio_therapy,
    f.record_origin,
    COUNT(*) AS patient_count,
    COUNT(*) FILTER (WHERE f.is_relapsed IS NOT NULL) AS known_relapse_count,
    COUNT(*) FILTER (WHERE f.is_relapsed = 1) AS relapsed_count,
    COUNT(*) FILTER (WHERE f.is_deceased IS NOT NULL) AS known_survival_status_count,
    COUNT(*) FILTER (WHERE f.is_deceased = 1) AS deceased_count,
    COUNT(*) FILTER (WHERE f.overall_survival_months IS NOT NULL) AS known_survival_duration_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.overall_survival_months)
        FILTER (WHERE f.overall_survival_months IS NOT NULL) AS median_survival_months,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.tumor_size)
        FILTER (WHERE f.tumor_size IS NOT NULL) AS median_tumor_size
FROM fact_clinical_outcomes_v2 f
JOIN dim_molecular_subtypes m ON m.molecular_subtype_key = f.molecular_subtype_key
JOIN dim_treatments t ON t.treatment_key = f.treatment_key
GROUP BY
    m.pam50_subtype, t.type_of_breast_surgery, t.chemotherapy,
    t.hormone_therapy, t.radio_therapy, f.record_origin;
