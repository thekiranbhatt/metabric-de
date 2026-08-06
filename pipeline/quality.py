import logging
from collections import Counter

logger = logging.getLogger(__name__)


class DataQualityException(Exception):
    pass


VALID_VITAL_STATUSES = {'Living', 'Died of Disease', 'Died of Other Causes'}


def validate_clinical_integrity(row):
    if row['vital_status'] is not None and row['vital_status'] not in VALID_VITAL_STATUSES:
        raise DataQualityException(f"Invalid vital_status '{row['vital_status']}' for {row['patient_id']}")

    if row['tumor_size'] is not None and float(row['tumor_size']) <= 0:
        raise DataQualityException(f"Non-positive tumor_size for {row['patient_id']}")

    if row['relapse_free_status_months'] is not None and float(row['relapse_free_status_months']) < 0:
        raise DataQualityException(
            f"Negative relapse_free_status_months for {row['patient_id']}"
        )

    if row['nottingham_prognostic_index'] is not None:
        npi = float(row['nottingham_prognostic_index'])
        if npi < 1.0:
            raise DataQualityException(f"NPI {npi} below the 1.0 clinical minimum for {row['patient_id']}")
        if npi > 7.5:
            logger.warning(
                "NPI review flag: patient %s has NPI %s above 7.5; retaining the record for human review",
                row['patient_id'],
                npi,
            )

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


def flag_relapse_endpoint_completeness(row):
    """Return a non-fatal completeness warning for the relapse endpoint, if any.

    A known recurrence without a recorded duration remains a valid source fact.
    It is eligible for status-based reporting but not for duration or KM analyses.
    """
    status = row['relapse_free_status']
    duration = row['relapse_free_status_months']

    if status == 'Recurred' and duration is None:
        return 'RECURRENCE_TIME_MISSING'
    if status == 'Not Recurred' and duration is None:
        return 'RELAPSE_FOLLOWUP_TIME_MISSING'
    if status is None and duration is not None:
        return 'RELAPSE_STATUS_MISSING_WITH_DURATION'
    if status is None and duration is None:
        return 'RELAPSE_ENDPOINT_UNAVAILABLE'
    return None


def run_quality_gate(records):
    clean, rejected = [], []
    warning_counts = Counter()
    for row in records:
        flag_block_missing_cohort(row)  # runs for every row, before the fatal check can short-circuit it
        warning_code = flag_relapse_endpoint_completeness(row)
        if warning_code:
            warning_counts[warning_code] += 1
            logger.warning(
                "Quality warning [%s]: patient=%s cohort=%s origin=%s; "
                "relapse status=%r, relapse-free months=%r",
                warning_code,
                row['patient_id'],
                row['cohort'],
                'augmented' if str(row['patient_id']).startswith('SYN-') else 'original',
                row['relapse_free_status'],
                row['relapse_free_status_months'],
            )
        try:
            validate_clinical_integrity(row)
            clean.append(row)
        except DataQualityException as e:
            logger.error(f"Row rejected: {e}")
            rejected.append((row.get('patient_id'), str(e)))
    logger.info(f"Quality gate: {len(clean)} passed, {len(rejected)} rejected")
    if warning_counts:
        logger.info(
            "Relapse endpoint completeness warnings: %s",
            ', '.join(f"{code}={count}" for code, count in sorted(warning_counts.items())),
        )
    return clean, rejected
