-- Gold V2 mirrors Silver's endpoint-completeness policy.  Rows with a known
-- relapse event but no duration remain status-reportable and are excluded by
-- time-based queries through their NULL relapse_free_months value.
ALTER TABLE fact_clinical_outcomes_v2
    DROP CONSTRAINT IF EXISTS chk_v2_recurred_has_duration,
    DROP CONSTRAINT IF EXISTS chk_v2_relapse_months_nonnegative;

ALTER TABLE fact_clinical_outcomes_v2
    ADD CONSTRAINT chk_v2_relapse_months_nonnegative
    CHECK (relapse_free_months IS NULL OR relapse_free_months >= 0);
