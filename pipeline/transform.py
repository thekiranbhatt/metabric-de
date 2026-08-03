import math

AGE_BUCKETS = [
    (0, 40, "<40"), (40, 50, "40-49"), (50, 60, "50-59"),
    (60, 70, "60-69"), (70, 200, "70+"),
]


def derive_age_group(age):
    if age is None:
        return None
    for low, high, label in AGE_BUCKETS:
        if low <= age < high:
            return label
    return None


def derive_receptor_profile(er_status, pr_status, her2_status):
    if er_status is None or pr_status is None or her2_status is None:
        return "Unknown"
    if er_status == "Negative" and pr_status == "Negative" and her2_status == "Negative":
        return "Triple Negative"
    return "Other"


def clean_and_transform_record(raw_row):
    clean = {}
    for k, v in raw_row.items():
        if v is None or (isinstance(v, float) and math.isnan(v)) or (isinstance(v, str) and v.strip() == ""):
            clean[k] = None
        else:
            clean[k] = v

    for col in ('er_status', 'er_status_ihc'):
        if clean.get(col) == 'Positve':
            clean[col] = 'Positive'

    clean['pam50_subtype'] = clean.pop('pam50_claudin_low_subtype', None)
    clean['three_gene_subtype'] = clean.pop('three_gene_classifier_subtype', None)

    if clean.get('cohort') is not None:
        clean['cohort'] = int(clean['cohort'])

    clean['age_group'] = derive_age_group(clean.get('age_at_diagnosis'))
    clean['receptor_profile'] = derive_receptor_profile(
        clean.get('er_status'), clean.get('pr_status'), clean.get('her2_status')
    )

    return clean