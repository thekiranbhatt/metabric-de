CREATE TABLE IF NOT EXISTS silver_outcomes (
    patient_id                 VARCHAR PRIMARY KEY REFERENCES silver_patients(patient_id) ON DELETE CASCADE,
    overall_survival_status    VARCHAR,
    overall_survival_months    NUMERIC,
    relapse_free_status        VARCHAR,
    relapse_free_status_months NUMERIC,
    vital_status                VARCHAR,

    CONSTRAINT chk_vital_status CHECK (
        vital_status IS NULL OR vital_status IN ('Living', 'Died of Disease', 'Died of Other Causes')
    ),
    CONSTRAINT chk_overall_survival_status CHECK (
        overall_survival_status IS NULL OR overall_survival_status IN ('Living', 'Deceased')
    ),
    CONSTRAINT chk_relapse_status CHECK (
        relapse_free_status IS NULL OR relapse_free_status IN ('Not Recurred', 'Recurred')
    ),
    CONSTRAINT chk_relapse_months_nonnegative CHECK (
        relapse_free_status_months IS NULL OR relapse_free_status_months >= 0
    ),
    CONSTRAINT chk_relapse_not_exceed_survival CHECK (
        relapse_free_status_months IS NULL OR overall_survival_months IS NULL
        OR relapse_free_status_months <= overall_survival_months
    )
);
