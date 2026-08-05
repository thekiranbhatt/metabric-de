-- cohort is METABRIC's real source batch marker and serves as the warehouse
-- incremental watermark. NULL cohorts are retained by full loads only.
ALTER TABLE fact_patient_outcomes
    ADD COLUMN IF NOT EXISTS cohort INTEGER;

-- patient_id is the OLTP natural key. This makes incremental retries
-- idempotent, even if a task fails after inserting only part of a cohort.
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_patient_outcomes_patient_id
    ON fact_patient_outcomes (patient_id);

CREATE INDEX IF NOT EXISTS idx_fact_patient_outcomes_cohort
    ON fact_patient_outcomes (cohort);
