import logging

logger = logging.getLogger(__name__)


class DataQualityException(Exception):
    pass


VALID_VITAL_STATUSES = {'Living', 'Died of Disease', 'Died of Other Causes'}


def validate_clinical_integrity(row):
    if row['vital_status'] is not None and row['vital_status'] not in VALID_VITAL_STATUSES:
        raise DataQualityException(f"Invalid vital_status '{row['vital_status']}' for {row['patient_id']}")

    if row['tumor_size'] is not None and float(row['tumor_size']) <= 0:
        raise DataQualityException(f"Non-positive tumor_size for {row['patient_id']}")

    if row['relapse_free_status'] == 'Recurred' and row['relapse_free_status_months'] is None:
        raise DataQualityException(f"Recurred with no relapse_free_status_months for {row['patient_id']}")

    if row['nottingham_prognostic_index'] is not None:
        npi = float(row['nottingham_prognostic_index'])
        if not (1.0 <= npi <= 7.0):
            raise DataQualityException(f"NPI {npi} outside 1.0-7.0 clinical range for {row['patient_id']}")

    if row['relapse_free_status_months'] is not None and row['overall_survival_months'] is not None:
        if float(row['relapse_free_status_months']) > float(row['overall_survival_months']):
            raise DataQualityException(f"relapse_free_status_months exceeds overall_survival_months for {row['patient_id']}")

    if row['age_at_diagnosis'] is not None:
        age = float(row['age_at_diagnosis'])
        if not (0 <= age <= 120):
            raise DataQualityException(f"age_at_diagnosis {age} outside plausible 0-120 range for {row['patient_id']}")

    return True


def flag_block_missing_cohort(row):
    block_fields = [row['vital_status'], row['chemotherapy'], row['hormone_therapy'],
                     row['radio_therapy'], row['overall_survival_status']]
    if all(v is None for v in block_fields):
        logger.info(f"{row['patient_id']} (cohort {row['cohort']}): block-missing clinical annotation, known pattern")


def run_quality_gate(records):
    clean, rejected = [], []
    for row in records:
        flag_block_missing_cohort(row)  # runs for every row, before the fatal check can short-circuit it
        try:
            validate_clinical_integrity(row)
            clean.append(row)
        except DataQualityException as e:
            logger.error(f"Row rejected: {e}")
            rejected.append((row.get('patient_id'), str(e)))
    logger.info(f"Quality gate: {len(clean)} passed, {len(rejected)} rejected")
    return clean, rejected

