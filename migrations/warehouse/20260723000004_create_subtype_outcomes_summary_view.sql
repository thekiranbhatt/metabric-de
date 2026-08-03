CREATE OR REPLACE VIEW subtype_outcomes_summary AS
SELECT
    s.pam50_subtype,
    COUNT(f.patient_id) AS total_patients,
    COUNT(f.patient_id) FILTER (WHERE f.is_deceased = 1) AS deceased_count,
    COUNT(f.patient_id) FILTER (WHERE f.is_relapsed = 1) AS relapsed_count,
    ROUND(AVG(f.overall_survival_months) FILTER (WHERE f.is_deceased IS NOT NULL)::NUMERIC, 1) AS avg_survival_months,
    ROUND(AVG(f.nottingham_prognostic_index)::NUMERIC, 2) AS avg_npi
FROM fact_patient_outcomes f
JOIN dim_subtype s ON f.subtype_key = s.subtype_key
GROUP BY s.pam50_subtype;