"""Streamlit presentation layer for the governed METABRIC clinical dashboard."""

from __future__ import annotations

from contextlib import contextmanager

import pandas as pd
import plotly.express as px
import streamlit as st

try:  # Supports direct Streamlit execution and package-style test runners.
    from app import queries
    from app.db import clear_connection_cache
    from app.embedded_db import embedded_postgres_enabled, start_embedded_postgres
except ModuleNotFoundError:  # pragma: no cover - used by `streamlit run app/main.py`
    import queries
    from db import clear_connection_cache
    from embedded_db import embedded_postgres_enabled, start_embedded_postgres


st.set_page_config(page_title="METABRIC Clinical Dashboard", page_icon="◈", layout="wide")

SLATE = "#2C3E50"
STEEL = "#5B7C99"
PALE_BLUE = "#8DA9C4"
EVENT_RED = "#C0392B"
TEAL = "#5C8583"
OCHRE = "#A87A37"
GRID = "#D8E0E6"


def pct(numerator: float, denominator: float) -> str:
    return "—" if not denominator else f"{(numerator / denominator) * 100:.1f}%"


def number(value: float | int | None, digits: int = 0) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.{digits}f}"


def text_value(value: object | None, fallback: str = "Unrecorded") -> str:
    """Return a Streamlit-safe text value for nullable database fields."""
    if value is None or pd.isna(value):
        return fallback
    return str(value)


def section_heading(kicker: str, title: str, description: str) -> None:
    st.markdown(
        f"""<div class="section-heading">
            <div class="section-kicker">{kicker}</div>
            <div><h2>{title}</h2><p>{description}</p></div>
        </div>""",
        unsafe_allow_html=True,
    )


def metric(label: str, value: str, detail: str) -> None:
    st.metric(label, value)
    st.caption(detail)


@contextmanager
def figure_card(title: str, description: str | None = None):
    """Create one restrained analytical surface for a chart, table, or figure."""
    card_key = "figure-" + "".join(character if character.isalnum() else "-" for character in title.lower())
    with st.container(border=True, key=card_key):
        st.markdown(f"<div class='figure-card-title'>{title}</div>", unsafe_allow_html=True)
        if description:
            st.markdown(f"<div class='figure-card-description'>{description}</div>", unsafe_allow_html=True)
        yield


def style_chart(fig, height: int = 340):
    fig.update_layout(
        height=height,
        margin=dict(l=12, r=12, t=30, b=12),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans, Avenir Next, Helvetica Neue, sans-serif", color=SLATE),
        legend_title_text="",
        hoverlabel=dict(font_family="IBM Plex Sans, Avenir Next, sans-serif"),
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID, title_standoff=10)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, title_standoff=10)
    return fig


st.markdown(
    """
    <style>
    :root { --slate: #2C3E50; --steel: #5B7C99; --pale: #8DA9C4; --event: #C0392B; --paper: #E8EAF0; --panel: #FFFFFF; --soft: #F2F4F8; --line: #CCD3DC; --muted: #596B78; }
    .stApp, [data-testid='stAppViewContainer'] { background: var(--paper); color: var(--slate); font-family: 'IBM Plex Sans', 'Avenir Next', 'Helvetica Neue', sans-serif; }
    [data-testid='stAppViewContainer'] p, [data-testid='stAppViewContainer'] li, [data-testid='stCaptionContainer'] { color: var(--muted) !important; }
    h1, h2, h3, [data-testid='stHeadingWithActionElements'] h1, [data-testid='stHeadingWithActionElements'] h2 { color: var(--slate) !important; font-family: 'IBM Plex Sans', 'Avenir Next', 'Helvetica Neue', sans-serif !important; font-weight: 650 !important; letter-spacing: -.025em; }
    h1, [data-testid='stHeadingWithActionElements'] h1 { font-size: 2.3rem !important; line-height: 1.12 !important; margin-bottom: .15rem !important; }
    [data-testid='stMetric'] { background: var(--panel); border: 1px solid var(--line); border-radius: 7px; padding: .9rem 1rem; box-shadow: 0 1px 2px rgba(44, 62, 80, .05); }
    [data-testid='stMetricLabel'] { color: var(--muted) !important; font-size: .72rem; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; }
    [data-testid='stMetricValue'], [data-testid='stMetricValue'] * { color: var(--slate) !important; font-family: 'IBM Plex Sans', 'Avenir Next', sans-serif !important; font-weight: 650; }
    .eyebrow, .section-kicker { color: var(--steel) !important; font-size: .72rem; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; }
    .section-heading { display: grid; grid-template-columns: 5.4rem 1fr; gap: .85rem; align-items: start; margin: 3.35rem 0 1.25rem; padding-top: .6rem; border-top: 1px solid #AEB9C5; }
    .section-kicker { padding-top: .7rem; }
    .section-heading h2 { font-size: 1.5rem !important; line-height: 1.2 !important; margin: .48rem 0 .22rem !important; }
    .section-heading p { margin: 0 0 .45rem !important; font-size: .94rem; max-width: 46rem; }
    .figure-card-title { color: var(--slate); font-size: 1rem; line-height: 1.25; font-weight: 700; letter-spacing: -.01em; margin: .08rem 0 .12rem; }
    .figure-card-description { color: var(--muted); font-size: .82rem; line-height: 1.35; margin: 0 0 .35rem; }
    .governance-note { border-left: 3px solid var(--steel); background: #F2F6F8; padding: .78rem .95rem; color: var(--slate) !important; font-size: .9rem; }
    .technical-note { border-left: 3px solid var(--pale); background: #F5F7F8; padding: .78rem .95rem; color: var(--muted) !important; font-size: .88rem; }
    .summary-label { color: var(--muted); font-size: .84rem; }
    div[data-testid='stSidebar'] { background: var(--slate); }
    div[data-testid='stSidebar'] h1, div[data-testid='stSidebar'] h2, div[data-testid='stSidebar'] h3, div[data-testid='stSidebar'] p, div[data-testid='stSidebar'] span, div[data-testid='stSidebar'] label { color: #F8FAFB !important; }
    div[data-testid='stSidebar'] [data-testid='stAlert'] { background: #E7EEF2; border: 0; }
    div[data-testid='stSidebar'] [data-testid='stAlert'] * { color: var(--slate) !important; }
    button[kind='secondary'] { border-color: #9AAEBC !important; }
    [data-baseweb='tab-list'] { gap: 1.25rem; border-bottom: 1px solid var(--line); }
    [data-baseweb='tab'] { color: var(--muted); font-weight: 650; padding: .7rem .1rem .6rem; }
    [aria-selected='true'][data-baseweb='tab'] { color: var(--slate) !important; border-bottom-color: var(--steel) !important; }
    [data-testid='stVerticalBlockBorderWrapper']:has(> div > [data-testid='stVerticalBlock'].st-key-figure) { background: var(--panel); border-color: var(--line) !important; border-radius: 7px; box-shadow: 0 1px 2px rgba(44, 62, 80, .05); }
    [data-testid='stPlotlyChart'] { background: transparent; padding: .1rem 0 0; }
    [data-testid='stDataFrame'] { border: 0; overflow: hidden; }
    [data-testid='stExpander'] { background: var(--panel); border: 1px solid var(--line); border-radius: 7px; box-shadow: 0 1px 2px rgba(44, 62, 80, .04); }
    </style>
    """,
    unsafe_allow_html=True,
)

if embedded_postgres_enabled():
    with st.status("Preparing the self-contained METABRIC database…", expanded=True) as database_status:
        try:
            embedded_runtime = start_embedded_postgres(database_status.write)
        except Exception as error:
            database_status.update(label="Embedded PostgreSQL failed to start", state="error", expanded=True)
            st.exception(error)
            st.stop()
        database_status.update(
            label=f"METABRIC database ready in {embedded_runtime.boot_seconds:.1f}s",
            state="complete",
            expanded=False,
        )

try:
    overview = queries.get_study_overview().iloc[0]
    kpis = queries.get_outcome_kpis().iloc[0]
    filter_options = queries.get_chart_filter_options()
except Exception as error:
    st.error("The dashboard could not connect to the METABRIC databases. Check the `.env` settings and run the pipeline.")
    st.exception(error)
    st.stop()

with st.sidebar:
    st.markdown("## METABRIC")
    st.caption("Clinical dashboard")
    st.divider()
    st.info("This warehouse contains augmented rows for ETL-scale testing. Clinical KPIs and insights use original source records only.")
    st.markdown("#### Chart filters")
    selected_pam50 = st.multiselect("PAM50 subtype", filter_options["pam50_subtype"])
    selected_age = st.multiselect("Age at diagnosis", filter_options["age_group"])
    selected_grade = st.multiselect("Histological grade", filter_options["histologic_grade"])
    selected_chemo = st.multiselect("Chemotherapy", filter_options["chemotherapy"])
    st.caption("Filters update charts and comparison tables. The headline KPIs remain the full original cohort.")
    st.divider()
    if st.button("Refresh dashboard data", use_container_width=True):
        st.cache_data.clear()
        clear_connection_cache()
        st.rerun()

filters: queries.ClinicalFilters = (
    tuple(selected_pam50), tuple(selected_age), tuple(selected_grade), tuple(selected_chemo)
)
filter_active = any(filters)

st.markdown("<div class='eyebrow'>Warehouse-backed reporting · original source records</div>", unsafe_allow_html=True)
st.title("METABRIC Clinical Dashboard")
st.caption("Clinical outcomes, risk profile, and governed data coverage.")

overview_tab, outcomes_tab, drilldown_tab = st.tabs(["Overview", "Outcomes & Risk", "Patient Drill-Down"])

with overview_tab:
    section_heading("Overview", "Study at a Glance", "Clinical measures show the data coverage that supports them.")
    cards = st.columns(4)
    with cards[0]:
        metric("Original patients", number(overview.original_patients), "Full original source cohort; augmented records excluded.")
    with cards[1]:
        metric("Relapse status known", number(overview.known_relapse_status), f"{pct(overview.known_relapse_status, overview.original_patients)} of original records")
    with cards[2]:
        metric("Median survival / follow-up", f"{number(kpis.median_survival_months, 1)} mo", f"{number(kpis.survival_duration_known)} records with duration")
    with cards[3]:
        metric("Median NPI", number(kpis.median_npi, 2), f"{number(kpis.npi_known)} records with recorded NPI")

    coverage = queries.get_endpoint_coverage(filters)
    coverage["coverage_pct"] = coverage["known"] / coverage["total"] * 100
    coverage_fig = px.bar(coverage, x="coverage_pct", y="measure", orientation="h", text=coverage.apply(lambda row: f"{int(row.known):,} / {int(row.total):,}", axis=1), color_discrete_sequence=[STEEL])
    coverage_fig.update_layout(showlegend=False, xaxis_title="Records with a known value (%)", yaxis_title="")
    coverage_fig.update_xaxes(range=[0, 100], ticksuffix="%")
    with figure_card("Clinical Data Coverage", "Status and duration are measured separately; missing time does not erase a known event."):
        st.plotly_chart(style_chart(coverage_fig, 300), use_container_width=True)
        st.caption("Chart filters apply here." if filter_active else "Coverage uses the full original-record cohort.")

    section_heading("02", "Who Is Represented?", "Composition views describe the available original clinical records; they do not imply outcome differences.")
    subtype_distribution = queries.get_subtype_distribution(filters)
    receptor_distribution = queries.get_receptor_profile_distribution(filters)
    mix_left, mix_right = st.columns(2)
    with mix_left:
        subtype_fig = px.bar(subtype_distribution, x="patient_count", y="pam50_subtype", orientation="h", text="patient_count", color_discrete_sequence=[STEEL])
        subtype_fig.update_layout(showlegend=False, xaxis_title="Original patients", yaxis_title="PAM50 subtype", yaxis={"categoryorder": "total ascending"})
        with figure_card("PAM50 Subtype", "Molecular subtype composition of the original clinical cohort."):
            st.plotly_chart(style_chart(subtype_fig), use_container_width=True)
            st.caption("Unrecorded subtype is shown explicitly rather than treated as a subtype.")
    with mix_right:
        receptor_fig = px.bar(receptor_distribution, x="patient_count", y="receptor_profile", orientation="h", text="patient_count", color_discrete_sequence=[TEAL])
        receptor_fig.update_layout(showlegend=False, xaxis_title="Original patients", yaxis_title="Derived receptor profile", yaxis={"categoryorder": "total ascending"})
        with figure_card("Receptor Profile", "A derived grouping from recorded ER, PR, and HER2 fields."):
            st.plotly_chart(style_chart(receptor_fig), use_container_width=True)
            st.caption("Derived from Gold receptor fields; unrecorded values remain visible.")

    st.markdown("#### Clinical Presentation")
    st.caption("Pathology and treatment composition are descriptive; treatment is not presented as evidence of effectiveness.")
    age_distribution = queries.get_age_distribution(filters)
    surgery_distribution = queries.get_surgery_distribution(filters)
    cancer_distribution = queries.get_cancer_type_distribution(filters)
    detailed_distribution = queries.get_cancer_type_detailed_distribution(filters)
    presentation_left, presentation_right = st.columns(2)
    with presentation_left:
        age_fig = px.bar(age_distribution, x="age_group", y="patient_count", text="patient_count", color_discrete_sequence=[PALE_BLUE])
        age_fig.update_layout(showlegend=False, xaxis_title="Age at diagnosis", yaxis_title="Original patients")
        with figure_card("Age at Diagnosis"):
            st.plotly_chart(style_chart(age_fig), use_container_width=True)
        surgery_fig = px.bar(surgery_distribution, x="surgery_type", y="patient_count", text="patient_count", color_discrete_sequence=[OCHRE])
        surgery_fig.update_layout(showlegend=False, xaxis_title="Type of breast surgery", yaxis_title="Original patients")
        with figure_card("Type of Breast Surgery"):
            st.plotly_chart(style_chart(surgery_fig), use_container_width=True)
    with presentation_right:
        cancer_fig = px.bar(cancer_distribution, x="patient_count", y="cancer_type", orientation="h", text="patient_count", color_discrete_sequence=[STEEL])
        cancer_fig.update_layout(showlegend=False, xaxis_title="Original patients", yaxis_title="Cancer type", yaxis={"categoryorder": "total ascending"})
        with figure_card("Cancer Type"):
            st.plotly_chart(style_chart(cancer_fig), use_container_width=True)
        detailed_fig = px.bar(detailed_distribution, x="patient_count", y="cancer_type_detailed", orientation="h", text="patient_count", color_discrete_sequence=[PALE_BLUE])
        detailed_fig.update_layout(showlegend=False, xaxis_title="Original patients", yaxis_title="Detailed cancer type", yaxis={"categoryorder": "total ascending"})
        with figure_card("Detailed Cancer Type"):
            st.plotly_chart(style_chart(detailed_fig, 390), use_container_width=True)

    treatment_distribution = queries.get_treatment_combination_distribution(filters)
    treatment_fig = px.bar(treatment_distribution.sort_values("patient_count"), x="patient_count", y="treatment_combination", orientation="h", text="patient_count", color_discrete_sequence=[TEAL])
    treatment_fig.update_layout(showlegend=False, xaxis_title="Original patients", yaxis_title="Treatment combination (top 10)")
    with figure_card("Treatment Combination", "The ten most common recorded combinations in the original cohort."):
        st.plotly_chart(style_chart(treatment_fig, 410), use_container_width=True)
        st.caption("Unknown treatment fields remain a distinct source-data category.")

    with st.expander("Warehouse provenance · technical context", expanded=False):
        st.markdown("<div class='technical-note'>Source-batch coverage includes augmented records and demonstrates warehouse loading behavior; it is not a clinical time-series.</div>", unsafe_allow_html=True)
        provenance, cohort_counts = queries.get_warehouse_provenance()
        provenance_cols = st.columns(2)
        provenance_cols[0].metric("Current warehouse fact count", number(provenance.iloc[0].fact_count))
        provenance_cols[1].metric("Maximum loaded cohort watermark", number(provenance.iloc[0].max_loaded_cohort))
        cohort_fig = px.bar(cohort_counts, x="cohort", y="record_count", text="record_count", color_discrete_sequence=[PALE_BLUE])
        cohort_fig.update_layout(showlegend=False, xaxis_title="Source batch (cohort)", yaxis_title="Warehouse records")
        st.plotly_chart(style_chart(cohort_fig), use_container_width=True)

with outcomes_tab:
    section_heading("03", "Known-Status Outcomes", "Event rates use records with known status; they never treat missing status as a negative event.")
    outcome_kpis = st.columns(3)
    with outcome_kpis[0]:
        metric("Relapse rate", pct(kpis.relapsed_count, kpis.relapse_known), f"{number(kpis.relapsed_count)} of {number(kpis.relapse_known)} with known relapse status")
    with outcome_kpis[1]:
        metric("Deceased rate", pct(kpis.deceased_count, kpis.survival_status_known), f"{number(kpis.deceased_count)} of {number(kpis.survival_status_known)} with known survival status")
    with outcome_kpis[2]:
        metric("Median survival / follow-up", f"{number(kpis.median_survival_months, 1)} mo", f"{number(kpis.survival_duration_known)} records with duration")
    st.caption("Headline outcome KPIs use the full original cohort; chart filters apply to the figures below.")
    outcome_status = queries.get_outcome_status_distribution(filters)
    subtype_outcomes = queries.get_subtype_outcomes(filters)
    outcome_left, outcome_right = st.columns([.62, 1.38])
    with outcome_left:
        status_fig = px.pie(outcome_status, names="survival_status", values="patient_count", hole=.62, color="survival_status", color_discrete_map={"Deceased": EVENT_RED, "Living": STEEL})
        status_fig.update_traces(textposition="inside", textinfo="percent+label")
        status_fig.update_layout(showlegend=False)
        known_status = outcome_status.patient_count.sum()
        with figure_card("Overall-Survival Status", "The dashboard's single intentional composition chart."):
            st.plotly_chart(style_chart(status_fig, 330), use_container_width=True)
            st.caption(f"Deceased/living composition among {number(known_status)} records with known overall-survival status.")
    with outcome_right:
        rate_frame = subtype_outcomes[["pam50_subtype", "relapsed_count", "relapse_known", "deceased_count", "survival_status_known"]].copy()
        rate_frame["Relapse rate"] = rate_frame.relapsed_count / rate_frame.relapse_known * 100
        rate_frame["Deceased rate"] = rate_frame.deceased_count / rate_frame.survival_status_known * 100
        rate_frame = rate_frame.melt(id_vars="pam50_subtype", value_vars=["Relapse rate", "Deceased rate"], var_name="Outcome", value_name="Rate (%)")
        rate_fig = px.bar(rate_frame, x="Rate (%)", y="pam50_subtype", orientation="h", color="Outcome", barmode="group", text_auto=".1f", color_discrete_map={"Relapse rate": EVENT_RED, "Deceased rate": STEEL})
        rate_fig.update_layout(xaxis_title="Rate among known-status records (%)", yaxis_title="PAM50 subtype", yaxis={"categoryorder": "total ascending"})
        with figure_card("PAM50 Outcome Comparison", "Grouped rates retain a distinct denominator for each subtype and outcome."):
            st.plotly_chart(style_chart(rate_fig, 390), use_container_width=True)
            st.caption("Subtype-level rates use their own known-status denominators in the table below.")

    outcome_display = subtype_outcomes.copy()
    outcome_display["Relapse rate · known"] = [
        f"{pct(row.relapsed_count, row.relapse_known)} · {int(row.relapse_known):,}"
        for _, row in outcome_display.iterrows()
    ]
    outcome_display["Deceased rate · known"] = [
        f"{pct(row.deceased_count, row.survival_status_known)} · {int(row.survival_status_known):,}"
        for _, row in outcome_display.iterrows()
    ]
    outcome_display["Median survival · duration"] = [
        f"{number(row.median_survival_months, 1)} mo · {int(row.survival_duration_known):,}"
        for _, row in outcome_display.iterrows()
    ]
    outcome_display["Median NPI · known"] = [
        f"{number(row.median_npi, 2)} · {int(row.npi_known):,}"
        for _, row in outcome_display.iterrows()
    ]
    if outcome_display.empty:
        st.info("No recorded PAM50 subtypes match the current chart filters.")
    else:
        with figure_card("Subtype Outcome Table", "A compact, denominator-aware comparison for biological subtypes."):
            st.dataframe(outcome_display[["pam50_subtype", "original_patients", "Relapse rate · known", "Deceased rate · known", "Median survival · duration", "Median NPI · known"]].rename(columns={"pam50_subtype": "PAM50 subtype", "original_patients": "Original patients"}), hide_index=True, use_container_width=True)
            st.caption("Each outcome cell is formatted as value · known-value denominator.")

    st.markdown("#### Risk Profile")
    st.caption("Observed survival/follow-up is descriptive and not a defined-horizon survival rate.")
    npi_values = queries.get_npi_values(filters)
    survival_by_subtype = queries.get_survival_by_subtype(filters)
    risk_left, risk_right = st.columns(2)
    with risk_left:
        npi_fig = px.histogram(npi_values, x="nottingham_prognostic_index", nbins=18, color_discrete_sequence=[OCHRE])
        npi_fig.update_layout(showlegend=False, xaxis_title="Nottingham Prognostic Index", yaxis_title="Original patients")
        with figure_card("Nottingham Prognostic Index"):
            st.plotly_chart(style_chart(npi_fig), use_container_width=True)
            st.caption(f"NPI distribution among {number(len(npi_values))} filtered records with a recorded NPI.")
    with risk_right:
        survival_fig = px.box(survival_by_subtype, x="pam50_subtype", y="overall_survival_months", color="pam50_subtype", points="outliers", color_discrete_sequence=[STEEL, PALE_BLUE, TEAL, OCHRE, "#7E8790"])
        survival_fig.update_layout(showlegend=False, xaxis_title="PAM50 subtype", yaxis_title="Observed survival / follow-up (months)")
        with figure_card("Observed Survival / Follow-Up by PAM50"):
            st.plotly_chart(style_chart(survival_fig), use_container_width=True)
            st.caption(f"Distribution uses {number(len(survival_by_subtype))} records with both recorded PAM50 subtype and survival/follow-up duration.")

    age_outcomes = queries.get_age_outcomes(filters)
    age_outcomes["Relapse rate · known"] = [
        f"{pct(row.relapsed_count, row.relapse_known)} · {int(row.relapse_known):,}"
        for _, row in age_outcomes.iterrows()
    ]
    age_outcomes["Deceased rate · known"] = [
        f"{pct(row.deceased_count, row.survival_known)} · {int(row.survival_known):,}"
        for _, row in age_outcomes.iterrows()
    ]
    st.markdown("#### Age-Group Outcomes")
    if age_outcomes.empty:
        st.info("No age-group outcome records match the current chart filters.")
    else:
        with figure_card("Age-Group Outcome Table", "Descriptive association only; each rate has its own known-status denominator."):
            st.dataframe(age_outcomes[["age_group", "original_patients", "Relapse rate · known", "Deceased rate · known"]].rename(columns={"age_group": "Age at diagnosis", "original_patients": "Original patients"}), hide_index=True, use_container_width=True)

with drilldown_tab:
    section_heading("04", "Individual Clinical History", "Original-record detail is retained in Silver for row-level review; aggregated views above are Gold-based.")
    st.markdown("<div class='governance-note'>Patient lists and lookup validation exclude augmented <code>SYN-*</code> identifiers.</div>", unsafe_allow_html=True)
    patient_ids = queries.get_original_patient_ids()
    patient_id = st.selectbox("Select an original patient", patient_ids, index=None, placeholder="Choose a patient ID")
    if patient_id:
        detail = queries.get_patient_detail(patient_id)
        if detail.empty:
            st.warning("No original-record detail is available for this patient.")
        else:
            record = detail.iloc[0]
            with figure_card("Patient Snapshot", "A concise entry point before the full Silver clinical-history record."):
                snapshot = st.columns(4)
                snapshot[0].metric("Age at diagnosis", number(record.age_at_diagnosis, 1))
                snapshot[1].metric("PAM50 subtype", text_value(record.pam50_subtype))
                snapshot[2].metric("Tumour stage", text_value(record.tumor_stage))
                snapshot[3].metric("NPI", number(record.nottingham_prognostic_index, 2))
            detail_frame = detail.T.reset_index()
            detail_frame.columns = ["Field", "Value"]
            detail_frame["Value"] = detail_frame["Value"].map(lambda value: text_value(value, fallback="—"))
            with figure_card("Full Clinical History", "Silver retains detailed receptor, pathology, treatment, and outcome fields excluded from aggregate Gold reporting."):
                st.dataframe(detail_frame, hide_index=True, use_container_width=True)
    else:
        st.caption("Choose a patient to view detailed pathology, receptor, treatment, and outcome fields.")
