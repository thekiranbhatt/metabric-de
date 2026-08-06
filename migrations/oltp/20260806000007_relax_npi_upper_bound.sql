-- Preserve source NPI values above the expected range.  The Python quality gate
-- logs values above 7.5 for human review, while this database constraint retains
-- them and continues to protect against impossible negative values.
ALTER TABLE silver_tumor_pathology
    DROP CONSTRAINT IF EXISTS chk_npi_range;

ALTER TABLE silver_tumor_pathology
    ADD CONSTRAINT chk_npi_minimum
    CHECK (nottingham_prognostic_index IS NULL OR nottingham_prognostic_index >= 1.0);
