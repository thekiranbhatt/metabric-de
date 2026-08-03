CREATE TABLE IF NOT EXISTS silver_patients (
    patient_id                VARCHAR PRIMARY KEY,
    age_at_diagnosis          NUMERIC,
    sex                       VARCHAR NOT NULL,
    cohort                    INTEGER,
    diagnosis_date            DATE NOT NULL,
    inferred_menopausal_state VARCHAR,
    age_group                 VARCHAR,

    CONSTRAINT chk_sex CHECK (sex = 'Female'),
    CONSTRAINT chk_age_plausible CHECK (age_at_diagnosis IS NULL OR age_at_diagnosis BETWEEN 0 AND 120)
);