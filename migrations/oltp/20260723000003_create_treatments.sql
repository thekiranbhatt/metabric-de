CREATE TABLE IF NOT EXISTS silver_treatments (
    patient_id             VARCHAR PRIMARY KEY REFERENCES silver_patients(patient_id) ON DELETE CASCADE,
    type_of_breast_surgery VARCHAR,
    chemotherapy            VARCHAR,
    radio_therapy           VARCHAR,
    hormone_therapy         VARCHAR,

    CONSTRAINT chk_chemotherapy CHECK (chemotherapy IS NULL OR chemotherapy IN ('Yes', 'No')),
    CONSTRAINT chk_radio_therapy CHECK (radio_therapy IS NULL OR radio_therapy IN ('Yes', 'No')),
    CONSTRAINT chk_hormone_therapy CHECK (hormone_therapy IS NULL OR hormone_therapy IN ('Yes', 'No'))
);