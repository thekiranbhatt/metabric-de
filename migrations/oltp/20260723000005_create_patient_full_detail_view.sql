CREATE OR REPLACE VIEW patient_full_detail AS
SELECT
    p.patient_id, p.age_at_diagnosis, p.age_group, p.sex, p.cohort,
    t.cancer_type, t.tumor_size, t.tumor_stage, t.receptor_profile,
    tr.type_of_breast_surgery, tr.chemotherapy, tr.hormone_therapy, tr.radio_therapy,
    o.vital_status, o.overall_survival_months, o.relapse_free_status
FROM silver_patients p
JOIN silver_tumor_pathology t ON p.patient_id = t.patient_id
JOIN silver_treatments tr ON p.patient_id = tr.patient_id
JOIN silver_outcomes o ON p.patient_id = o.patient_id;