CREATE TABLE IF NOT EXISTS fact_patient_outcomes (
    fact_id                     SERIAL PRIMARY KEY,
    patient_id                  VARCHAR NOT NULL,
    date_key                    INT NOT NULL REFERENCES dim_date(date_key),
    subtype_key                 INT NOT NULL REFERENCES dim_subtype(subtype_key),
    age_at_diagnosis            NUMERIC,
    tumor_size                  NUMERIC,
    mutation_count               INT,
    nottingham_prognostic_index NUMERIC,
    overall_survival_months     NUMERIC,
    relapse_free_months         NUMERIC,
    is_deceased                  INT,
    is_relapsed                  INT,
    age_group                    VARCHAR,
    receptor_profile             VARCHAR,

    CONSTRAINT chk_is_deceased_range CHECK (is_deceased IS NULL OR is_deceased IN (0, 1)),
    CONSTRAINT chk_is_relapsed_range CHECK (is_relapsed IS NULL OR is_relapsed IN (0, 1))
);