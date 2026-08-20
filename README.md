# METABRIC Clinical Data Engineering Platform

A data engineering project built on the METABRIC breast cancer clinical dataset. Raw clinical data moves through Bronze, Silver, and Gold layers, is orchestrated with Airflow, and is presented through a governed Streamlit dashboard.

The project covers a full pipeline: relational data modeling, ETL across two databases, data quality checks, reliable warehouse loading, and clear handling of missing and augmented data.

## Architecture

![METABRIC data architecture and flow](assets/metabric-data-architecture.png)

| Layer | Store | Responsibility |
| --- | --- | --- |
| Bronze | `staging_metabric` | Source CSV staging |
| Silver | `metabric_prod` | Clean, normalized patient, pathology, treatment, and outcome records |
| Gold | `metabric_warehouse` | Analytical facts and dimensions for reporting |
| Presentation | Streamlit | Gold-based summaries and Silver-level patient review |

Two separate PostgreSQL databases are used on purpose. Since they can't be joined directly with SQL, the warehouse loader handles those relationships in Python — a common boundary in real ETL systems.

## Data model

The Gold layer holds the current clinical star schema used by the dashboard. Its main fact table stores clinical measures, survival and relapse outcomes, batch information, and derived event flags. It connects to dimension tables for patients, tumor characteristics, molecular subtypes, and treatments.

An earlier Gold model is still kept alongside the current one. Both are refreshed and compared during the transition, which gives a way to check results before switching over fully.

## Data handling

- The original dataset contains 2,509 records.
- Records prefixed `SYN-` are augmented records created only to test the pipeline at larger scale. They are excluded from clinical analysis.
- All clinical figures in the dashboard are filtered to original records, and this is shown clearly in the UI.
- Rates and other statistics always show what they're based on. Missing data is never treated as a negative outcome.
- Synthetic dates are excluded from analysis. The `cohort` field is a source-batch marker, not a clinical timeline.
- A `record_origin` field marks each record as `original` or `augmented`, so the distinction is built into the data itself.

## How the pipeline runs

Silver is rebuilt from the source data on every run. Gold supports two modes:

- `full` — rebuilds the warehouse from scratch. Used for setup or recovery.
- `incremental` — loads only new data since the last run. Safe to run repeatedly after an initial full load.

Each Gold model loads inside its own transaction. Airflow checks both models after loading and compares record counts and data coverage before marking a run successful.

## Dashboard

The Streamlit app has three views:

- **Overview** — data coverage, cohort makeup, pathology, treatment, and warehouse provenance
- **Outcomes & Risk** — outcome summaries and comparisons across subtypes and risk groups
- **Patient Drill-Down** — detailed patient records from Silver, including fields not part of the Gold summaries

SQL queries are kept in `app/queries.py`, separate from the app's display logic in `app/main.py`.

## Preview

<p align="center">
  <a href="assets/dashboard-overview.jpg"><img src="assets/dashboard-overview.jpg" alt="METABRIC dashboard overview with governed coverage and cohort KPIs" width="49%"></a>
  <a href="assets/dashboard-outcomes-risk.jpg"><img src="assets/dashboard-outcomes-risk.jpg" alt="METABRIC outcomes and risk view with survival-status and PAM50 charts" width="49%"></a>
</p>

<a href="assets/airflow-expanded-dag.jpg"><img src="assets/airflow-expanded-dag.jpg" alt="Expanded Airflow DAG showing Silver refresh, warehouse sync, and validation reporting task groups"></a>

The DAG runs daily, supports both full and incremental Gold loads, and can also be triggered manually for a demo or recovery run.

## Project structure

![METABRIC project structure](assets/metabric-project-structure.png)

## Running locally

Create the `metabric_prod` and `metabric_warehouse` databases, copy `.env.example` to `.env`, and fill in both database configurations. Place the source file at `data/staging_metabric.csv`.

```bash
.venv/bin/python run_migrations.py --target oltp
.venv/bin/python run_migrations.py --target warehouse
.venv/bin/python scripts/load_staging.py
.venv/bin/python run_pipeline.py --mode full
.venv/bin/streamlit run app/main.py
```

After the initial load, use the incremental path when appropriate:

```bash
.venv/bin/python run_pipeline.py --mode incremental
```

To run orchestration locally:

```bash
docker compose up --build
```

Open the local Airflow web interface, unpause `metabric_oltp_to_warehouse`, and run it manually with `full` for initialization. It's scheduled daily on `incremental` afterward.

## Self-contained Streamlit deployment

For an occasional-use Streamlit deployment, the dashboard can boot an ephemeral PostgreSQL 17 instance with PGembed, load the bundled compressed source snapshot, and run the full pipeline before serving the first page. No external database service is required.

Enable it with one root-level Streamlit secret:

```toml
METABRIC_EMBEDDED_POSTGRES = "true"
```

The app prefers `/dev/shm` when at least 128 MB of memory-backed storage is available and otherwise uses the platform's temporary directory. The database lasts for the Streamlit process/container lifetime and is rebuilt automatically after the platform removes that container. The existing `SRC_DB_*` and `DEST_DB_*` settings remain unchanged for normal local or externally hosted PostgreSQL use.

## Stack

PostgreSQL · Python · pandas · psycopg2 · Faker · Apache Airflow · Docker Compose · Streamlit · Plotly

## Data source and use

Clinical data was sourced from Kaggle and originates from the METABRIC breast cancer study. This project is for educational data engineering practice only. It is not a clinical decision-support tool, and outcome comparisons are descriptive associations in an observational dataset, not treatment effects.
