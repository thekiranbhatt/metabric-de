-- A known relapse status and its event/follow-up time are separate source facts.
-- Preserve known status when the source duration is missing; only prohibit
-- impossible negative durations at the database boundary.
ALTER TABLE silver_outcomes
    DROP CONSTRAINT IF EXISTS chk_recurred_has_months;

ALTER TABLE silver_outcomes
    ADD CONSTRAINT chk_relapse_months_nonnegative
    CHECK (relapse_free_status_months IS NULL OR relapse_free_status_months >= 0);
