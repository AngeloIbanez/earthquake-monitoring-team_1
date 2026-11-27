# Natural Hazard Monitoring System — Earthquake Activity Monitoring
**Course:** Programación para Análisis de Datos (Data Analysis Programming) — Semester 2026-II  
**Project:** Natural Hazard Monitoring Systems — **Team 1: Earthquake Activity**  
**Repository:** `earthquake-monitoring-team_1`  
**Team members:**
- Juan Córdoba - 20211020097
- Mauricio Sánchez - 20212020045
- Angelo Ibáñez - 20212020007

[![Test Suite](https://img.shields.io/badge/tests-pytest%20verified-brightgreen.svg)]()
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)]()
[![Database](https://img.shields.io/badge/database-PostgreSQL%2016+-blue.svg)]()
[![PySpark](https://img.shields.io/badge/PySpark-3.5%2B%20local%5B*%5D-orange.svg)]()
[![Plotly Dash](https://img.shields.io/badge/Plotly%20Dash-2.14%2B-1f77b4.svg)]()
[![Status](https://img.shields.io/badge/Workshop%20%231-Completed-success.svg)]()
[![Status](https://img.shields.io/badge/Workshop%20%232-Completed-success.svg)]()
[![Status](https://img.shields.io/badge/Workshop%20%233-Completed-success.svg)]()
[![Status](https://img.shields.io/badge/Workshop%20%234-Completed-success.svg)]()

---

## 1. Executive Summary & Project Purpose

Earthquakes are sudden, high-consequence natural hazards caused by tectonic strain energy release along lithospheric faults. Unlike hydrometeorological phenomena, seismic ruptures cannot be predicted deterministically. Monitoring and civil protection depend on **near-instantaneous event detection**, **rapid multi-catalog telemetry reconciliation**, and **quantitative anomaly detection** to direct emergency response and civil defense operations.

This repository implements the end-to-end data engineering architecture for **Team 1**, standardizing and reconciling seismic telemetry from the world's leading seismological networks:
- **USGS (United States Geological Survey):** Global FDSN web services & real-time summary GeoJSON feeds.
- **EMSC (Euro-Mediterranean Seismological Centre):** European & Mediterranean FDSN seismic portal.
- **GDACS (Global Disaster Alert and Coordination System):** Multi-hazard emergency disaster alert feeds.

---

## 2. High-Level System Architecture

The system integrates a **Three-Layer Medallion Data Lakehouse** (`datalake_bronze/`, `datalake_silver/`, `datalake_gold/`) orchestrated by Apache Airflow 2.10.4 and PySpark (`local[*]`) with a **7-Table Canonical PostgreSQL Persistence & Reconciliation Engine** (`ingest.py` / `src/`). Multi-format raw payloads (GeoJSON, JSON, and RSS 2.0 XML) pass through strict schema and physical bounding gates, route malformed records to a `JSONB` quarantine table (`rejected_record`), resolve cross-catalog event duplication via **1-to-1 Nearest-Neighbor Spatio-Temporal Matching & Union-Find Transitive Grouping** (`event_group`), and consolidate into Gold Parquet summaries for data governance and functional user storytelling.

```mermaid
flowchart TD
    subgraph SOURCES["1. Multi-Format Observing Networks"]
        U_HIST["USGS FDSN Historical<br>(GeoJSON REST, M ≥ 2.5)"]
        E_HIST["EMSC FDSN Historical<br>(JSON REST, M ≥ 2.5)"]
        U_INC["USGS Real-Time Feed<br>(GeoJSON, all_day.geojson)"]
        G_INC["GDACS Alert Feed<br>(RSS 2.0 XML, rss_24h.xml)"]
    end

    subgraph BRONZE["2. Bronze Layer & Extraction Engine"]
        B_DAG["bronze_ingestion_dag.py<br>& ingest.py CLI"]
        B_STORE[("datalake_bronze/ & data/raw/<br>(Immutable JSON / GeoJSON / XML)")]
    end

    subgraph SILVER["3. Silver Layer & Canonical Validation"]
        S_DAG["silver_processing_dag.py<br>& src/parsers.py + src/models.py"]
        VAL{"Validation Gate<br>[-90,90], [-180,180]<br>-1.0 ≤ M ≤ 10.0"}
        S_PQ[("datalake_silver/<br>earthquakes_silver_*.parquet<br>(TIMESTAMP_MICROS)")]
        MATCH["src/matching.py<br>(1-to-1 Nearest-Neighbor &<br>Union-Find Transitive Grouping)"]
    end

    subgraph STORAGE["4. Canonical Database Layer (PostgreSQL 16)"]
        SRC[("source & ingestion_run")]
        ST[("ingestion_state (HWM)")]
        EV[("event & earthquake")]
        GRP[("event_group (637 Groups)")]
        REJ[("rejected_record (JSONB)")]
    end

    subgraph GOLD["5. Gold Layer (PySpark local[*]) & Presentation (Dash)"]
        G_DAG["gold_processing_dag.py<br>& src/gold_pyspark.py"]
        G_GOV[("datalake_gold/<br>governance_*.parquet")]
        G_STR[("datalake_gold/<br>storytelling_*.parquet")]
        GOV_DASH["dashboard/governance_app.py<br>(:8050 — Data Governance)"]
        STR_DASH["dashboard/storytelling_app.py<br>(:8051 — Seismic Storytelling)"]
    end

    U_HIST --> B_DAG
    E_HIST --> B_DAG
    U_INC --> B_DAG
    G_INC --> B_DAG

    B_DAG --> B_STORE
    B_STORE --> S_DAG
    S_DAG --> VAL
    VAL -->|Malformed / Out-of-bounds| REJ
    VAL -->|Valid Curated Records| S_PQ
    VAL -->|Valid Canonical Entities| MATCH

    MATCH --> EV
    MATCH --> GRP
    S_DAG --> SRC
    S_DAG --> ST

    S_PQ -->|spark.read.parquet| G_DAG
    G_DAG --> G_GOV
    G_DAG --> G_STR
    G_GOV -->|gold_loader.py| GOV_DASH
    G_STR -->|gold_loader.py| STR_DASH
```

---

## 3. Geophysical Foundations & Domain Understanding

A detailed, peer-reviewed geophysical foundation is documented in [`docs/hazard_definition.md`](docs/hazard_definition.md):
- **Elastic Rebound Theory:** Tectonic strain accumulation, Coulomb frictional failure, and dynamic rupture propagation at shear velocities ($v_r \approx 0.85 v_s$).
- **Wave Propagation Modes:**
  - *Compressional ($P$) waves:* Longitudinal, fastest arrival ($v_p \approx 6 - 8\text{ km/s}$), travels through solids and liquids.
  - *Shear ($S$) waves:* Transverse, high kinetic energy ($v_s \approx 3.5\text{ km/s}$), strictly requires shear rigidity ($\mu > 0$, solids only).
  - *Surface waves (Rayleigh & Love):* Trapped in crustal waveguides, low attenuation ($\propto 1/\sqrt{r}$), dominant cause of high-rise structural resonance.
- **Hypocenter vs. Epicenter:** 3D focal coordinates $(x, y, z)$ vs. 2D surface geoid projection $(x, y, 0)$.
- **Focal Depth Regimes & PGA Attenuation:**
  - *Shallow ($0 - 70\text{ km}$):* High Peak Ground Acceleration (PGA), localized catastrophic destruction, tsunami generation potential.
  - *Intermediate ($70 - 300\text{ km}$):* Wadati-Benioff subduction zones.
  - *Deep ($> 300\text{ km}$):* Severe anelastic attenuation ($e^{-\omega R / 2vQ}$), low surface damage.
- **Magnitude Scale Saturation:** Body-wave ($m_b$) and local ($M_L$) scales saturate at $M \sim 6.0 - 6.5$ due to short-period 1-second filter limits. Moment magnitude ($M_w$) scales directly with physical seismic moment $M_0 = \mu A D$ without saturation.

---

## 4. Validated Source Payloads & Empirical Evidence

All four sources are fully configured in [`config/sources.yaml`](config/sources.yaml) and verified with multi-source raw sample payloads:

| Source ID | Institution | Format | Sample Payload File | Verified Records | Scope / Window |
|---|---|:---:|---|:---:|---|
| `usgs_fdsn` | USGS (NEIC) | GeoJSON | [`workshop_1/samples/usgs_historical_raw.json`](workshop_1/samples/usgs_historical_raw.json) | **618** | Global historical ($M \ge 4.5$, 1 month) |
| `emsc_fdsn` | EMSC | JSON | [`workshop_1/samples/emsc_historical_raw.json`](workshop_1/samples/emsc_historical_raw.json) | **627** | Matching global window ($M \ge 4.5$) |
| `usgs_realtime` | USGS | GeoJSON | [`workshop_1/samples/usgs_incremental_raw.json`](workshop_1/samples/usgs_incremental_raw.json) | **1,764** | Real-time rolling 7-day stream (`all_week`) |
| `gdacs_realtime` | GDACS (EU JRC) | RSS 2.0 XML | [`workshop_1/samples/gdacs_incremental_raw.xml`](workshop_1/samples/gdacs_incremental_raw.xml) | **25 (12 EQ)** | Active 24-hour disaster alert feed |

### Empirical Two-Poll Ingestion Characterization
Documented in [`workshop_1/polling_evidence.md`](workshop_1/polling_evidence.md):
- **Poll #1 (`05:33:48Z`):** 12 events captured from `all_hour.geojson`.
- **Poll #2 (`06:52:11Z`):** 10 events captured (**78 min elapsed**, exceeding the 60-minute rolling window).
- **Window Eviction:** $|P_1 \cap P_2| = 0$, proving sliding-window feeds drop events rapidly without persistent storage.
- **In-Place Revision Proof:** Queried USGS FDSN Web Service to empirically prove that event `aka2026werkhb` received phase-pick refinements (+24.7 min) and event `us7000tx1f` received DYFI/ShakeMap product updates (+62.4 min).
- **Design Decisions:** High-water mark tracks `properties.updated` (not origin time) with a **24-hour overlap window** and idempotent SQL upserts (`ON CONFLICT (event_id) DO UPDATE`).

---

## 5. Source Profiling & Quality Findings Summary

Executed and pre-evaluated in [`notebooks/01_source_profiling.ipynb`](notebooks/01_source_profiling.ipynb) and [`notebooks/03_silver_data_quality_findings.ipynb`](notebooks/03_silver_data_quality_findings.ipynb):
1. **GDACS XML Parser Defect Resolved:** Targeted `<georss:point>` and extracted magnitude/depth via regex on `<gdacs:severity>`, reducing null rates from 100% to **0.0%**.
2. **Comprehensive Profiling Matrix:** 100% coordinate compliance (all within $[-90, 90]$ and $[-180, 180]$) with zero duplicate IDs after canonical deduplication.
3. **5 Concrete Quality Issues:** Documented with exact raw snippets for heterogeneous timestamps, uncalibrated/negative depths, magnitude saturation, location naming asymmetry, and artificial 10.0 km pinned depths (affecting 45.1% of USGS historical events and 29.85% of the consolidated multi-source Silver corpus).
4. **Cross-Source Event Matching:**
   - Naive window matching produced 679 candidate matches with **72 1-to-N collisions** during earthquake swarms.
   - Applied **Normalized Spatio-Temporal Nearest-Neighbor Assignment** ($D = \sqrt{(\Delta d / 100\text{km})^2 + (\Delta t / 900\text{s})^2}$), resolving 100% of collisions into **568 clean 1-to-1 historical pairs** (and **637 multi-source transitive groups / 1,274 grouped records** across all four feeds) with median $\Delta t = 0.50\text{ s}$ and median $\Delta d = 6.97\text{ km}$.

---

## 6. Canonical Data Schema & Ingestion Contract

Detailed in [`workshop_1/canonical mapping table.md`](workshop_1/canonical%20mapping%20table.md) and implemented in [`sql/schema.sql`](sql/schema.sql):
- **Core Entities:** `source`, `ingestion_run`, `ingestion_state`, `event`, `earthquake`, `event_group`, `rejected_record`.
- **Severity Normalization:** Standardizes magnitude to equivalent moment magnitude $M_w^*$ (Scordilis, 2006) and computes radiated energy:
  $$E = 10^{4.8 + 1.5 M_w^*} \quad (\text{Joules})$$
- **Historical Volume Estimation:** 5-year historical scope yields **$\approx 1,800,000$ events** ($\approx 1.72\text{ GB}$ raw, $\approx 433\text{ MB}$ PostgreSQL storage with indexes).
- **Recursive Temporal Bisection:** Automatically bisects query intervals if aftershock clusters exceed the USGS 20,000-record (or EMSC 10,000-record) API limit.

---

## 7. Seismic Activity Anomaly Index (SAAI)

The system proposes the **Seismic Activity Anomaly Index (SAAI)** to quantify real-time departures from background seismicity:

$$\mathbf{SAAI}(t) = 0.4 \cdot \left(\frac{N_{24h}(t) - \mu_N}{\sigma_N}\right) + 0.6 \cdot \left(\frac{\log_{10} E_{24h}(t) - \mu_E}{\sigma_E}\right)$$

- $\text{SAAI} < 1.0$: **NORMAL (Green)** — Baseline seismicity.
- $1.0 \le \text{SAAI} < 2.0$: **ELEVATED (Yellow)** — Minor swarm activity.
- $2.0 \le \text{SAAI} < 3.5$: **HIGH (Orange)** — Significant swarm or moderate rupture.
- $\text{SAAI} \ge 3.5$: **CRITICAL (Red)** — Major megathrust rupture or destructive sequence.

---

## 8. Repository Layout

```text
earthquake-monitoring-team_1/
├── airflow/
│   └── dags/
│       ├── bronze_ingestion_dag.py     # Airflow DAG: Multi-source extraction (USGS, EMSC, GDACS) -> datalake_bronze/
│       ├── silver_processing_dag.py    # Airflow DAG: Cleansing, quarantine & Parquet/Postgres persistence
│       └── gold_processing_dag.py      # Airflow DAG: PySpark Silver-to-Gold governance & storytelling aggregations
├── config/
│   └── sources.yaml                    # Standardized catalog configuration (4 sources)
├── dashboard/
│   ├── __init__.py
│   ├── app.py                          # Legacy single-page Dash prototype (Workshop #1/#2)
│   ├── assets/
│   │   └── custom.css                  # Shared styling for Workshop #4 Dash applications
│   ├── components.py                   # Reusable UI cards, headers, and error banners
│   ├── figures_governance.py           # Plotly figures & KPI extractors for Governance (:8050)
│   ├── figures_storytelling.py         # Plotly figures & geo map builders for Storytelling (:8051)
│   ├── gold_loader.py                  # Exclusive Gold Parquet loader enforcing docs/gold_contract_w4.md
│   ├── governance_app.py               # Workshop #4 Data Governance Dashboard (http://localhost:8050)
│   ├── narrative.py                    # Dynamic plain-language geophysical narrative generator
│   ├── storytelling_app.py             # Workshop #4 Seismic Storytelling Dashboard (http://localhost:8051)
│   └── theme.py                        # Color palette, alert tier badges, and agency labels
├── datalake_bronze/                    # Medallion Bronze layer: raw immutable JSON/XML responses
├── datalake_silver/                    # Medallion Silver layer: curated Apache Parquet files
├── datalake_gold/                      # Medallion Gold layer: governance_*.parquet & storytelling_*.parquet
├── data/
│   └── raw/                            # Bulk landing zone (source_YYYYMMDD_HHMMSS.ext)
├── docs/
│   ├── gold_contract_w4.md             # Workshop #4 Gold Layer Data Contract & invariant rules
│   ├── hazard_definition.md            # Comprehensive geophysical domain & project formalization
│   ├── Project Definition Report.pdf   # Workshop #1 compiled PDF report
│   ├── Workshop_2_Technical_Architecture_Report.pdf
│   └── Workshop_3_Gold_Pipeline_Report.pdf
├── init-db/
│   └── 01-create-etl-database.sql      # Automated PostgreSQL DB and ETL user provisioning
├── notebooks/
│   ├── 01_source_profiling.ipynb                     # Workshop #1: source profiling & quality analysis
│   ├── 02_silver_data_exploration.ipynb              # Workshop #2: Silver Parquet reading & statistical profiling
│   ├── 03_silver_data_quality_findings.ipynb         # Workshop #3: Silver quality findings, null/pinned & IQR outliers
│   ├── 04_gold_summaries_exploration.ipynb           # Workshop #3: Gold Governance & Storytelling exploration
│   ├── 05_workshop4_gold_integration_validation.ipynb # Workshop #4: Gold contract & dashboard integration checks
│   ├── 06_workshop4_dashboard_design_lab.ipynb       # Workshop #4: visual prototyping & layout validation
│   └── 07_workshop4_narrative_claims_check.ipynb     # Workshop #4: quantitative audit of narrative claims
├── scripts/
│   └── validate_gold_contract.py       # Automated Gold Parquet schema & conservation validator
├── sql/
│   └── schema.sql                      # PostgreSQL production DDL (7 canonical tables + indexes)
├── src/
│   ├── __init__.py
│   ├── config.py                       # YAML & environment variable loader
│   ├── db.py                           # PostgreSQL canonical persistence, watermarks, quarantine & status
│   ├── extractors.py                   # Live HTTP partitioned extractor with recursive temporal bisection
│   ├── gold_pyspark.py                 # PySpark local[*] Silver-to-Gold transformation engine
│   ├── models.py                       # Canonical dataclass models & validation gating
│   ├── parsers.py                      # USGS GeoJSON, EMSC JSON, GDACS XML parsers with SHA-256 hashes
│   └── matching.py                     # Haversine, 1-to-1 Nearest-Neighbor & transitive event_group builder
├── tests/
│   ├── __init__.py
│   ├── test_parsers.py                 # Unit tests for multi-format parsers & quarantine validation reasons
│   ├── test_matching.py                # Unit tests for Haversine & collision resolution
│   ├── test_schema.py                  # Unit tests validating SQL schema constraints
│   ├── test_workshop3.py               # Unit tests for transitive grouping, watermark windows & Gold Parquets
│   ├── test_gold_w4.py                 # Unit tests for Workshop #4 additive Gold schema & metrics
│   └── test_workshop4.py               # Unit tests for gold_loader, figure builders & narrative claims
├── workshop_1/
│   ├── canonical mapping table.md      # Canonical mapping table, Mermaid ERD, and ingestion contract
│   ├── polling_evidence.md             # Empirical two-poll update evidence & diff analysis
│   └── samples/                        # Raw validated JSON/XML samples (≥ 200 records each)
├── workshop_2/
│   ├── Workshop_2_Technical_Architecture_Report.pdf
│   ├── Workshop_2_Technical_Architecture_Report.tex
│   ├── technical_architecture_report.md
│   ├── screenshots/                    # Verification screenshots of Airflow UI, datalake & notebooks
│   └── sections/                       # Modular LaTeX sections (00_abstract .. 08_references)
├── workshop_3/
│   ├── Workshop_3_Gold_Pipeline_Report.pdf    # Compiled IEEEtran PDF report for Workshop #3
│   ├── Workshop_3_Gold_Pipeline_Report.tex    # Master IEEEtran LaTeX report for Workshop #3
│   ├── figures/                               # Diagnostic plots generated from Silver and Gold Parquet datasets
│   └── sections/                              # Modular LaTeX sections (00_abstract .. 08_references)
├── workshop_4/
│   ├── paper/                          # Workshop #4 IEEEtran LaTeX report artifacts
│   └── screenshots/                    # Verification screenshots of Airflow & both Dash UIs
├── .env.example                        # Environment variable template (includes AIRFLOW_UID)
├── .gitignore                          # Python, data raw, and IDE ignores
├── docker-compose.yml                  # Full Docker stack (Airflow 2.10.4, Postgres 16, PySpark, 2 Dashboards)
├── Dockerfile                          # Custom Airflow + Java + PySpark + Plotly Dash image
├── ingest.py                           # Canonical Ingestion Worker CLI (historical | incremental | status)
├── requirements.txt                    # Core Python & dashboard dependencies
├── requirements-dev.txt                # Host development, notebook & Playwright verification dependencies
└── README.md                           # Master project overview & academic documentation
```

---

## 9. Quickstart & Execution Guide

### Prerequisites
- Python 3.10+ and Java OpenJDK 17/21 (for PySpark `local[*]` execution)
- Docker Engine & Docker Compose v2

### 1. Python Environment Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Optional: for notebook execution and Playwright visual verification on host
pip install -r requirements-dev.txt
```

### 2. Run Automated Test Suite (`pytest`) & Gold Contract Validation
Execute the full automated test suite (covering parsers, quarantine reasons, collision-free 1-to-1 and transitive matching, watermark overlap windows, SQL schema integrity, Gold Parquet additive schema, `gold_loader.py`, figure builders, and narrative claims):
```bash
pytest -v
```
Validate the latest Gold Parquet snapshots against all cross-dataset conservation and physical bound invariants defined in [`docs/gold_contract_w4.md`](docs/gold_contract_w4.md):
```bash
python scripts/validate_gold_contract.py
```

### 3. Canonical Ingestion CLI (`ingest.py` — Workshops #2 & #3)
```bash
# 1. Historical Mode (live partitioned HTTP fetch with recursive bisection)
python ingest.py --mode historical --start 2025-11-01 --end 2025-11-05

# 2. Historical & Incremental Mode using verified local samples (idempotent)
python ingest.py --mode historical --use-samples
python ingest.py --mode incremental --use-samples

# 3. Pipeline Status Report (table row counts, source watermarks, recent runs, event_group matches)
python ingest.py --mode status
```

### 4. End-to-End Containerized Stack (Docker Compose — Workshops #2, #3 & #4)
On Linux hosts, copy `.env.example` to `.env` and set `AIRFLOW_UID=$(id -u)` (e.g., `1000`) so Airflow and PySpark containers have write permissions to the mounted `datalake_*` directories. Then build and launch the full stack:
```bash
cp .env.example .env
# Edit .env to set AIRFLOW_UID=1000 on Linux
docker compose up -d --build
docker compose ps
```

| Service | Local URL / Endpoint | Credentials / Notes |
|---|---|---|
| **Apache Airflow Web UI** | `http://localhost:8080` | Username: `airflow` / Password: `airflow` |
| **Data Governance Dashboard** | `http://localhost:8050` | Standalone Dash UI (`dashboard/governance_app.py`) |
| **Seismic Storytelling Dashboard** | `http://localhost:8051` | Standalone Dash UI (`dashboard/storytelling_app.py`) |
| **PostgreSQL 16 (Canonical + Airflow)** | `localhost:5440` | ETL DB: `earthquakes_ud` (User: `etl_ud`) |

#### Medallion DAG Execution Order in Airflow
Trigger or schedule the three DAGs sequentially in the following order:
1. **`bronze_ingestion_dag`** (`*/15 * * * *`): Extracts multi-source raw telemetry (USGS GeoJSON, EMSC JSON, GDACS XML) into `datalake_bronze/`.
2. **`silver_processing_dag`** (`*/20 * * * *`): Triggered via `FileSensor` on `datalake_bronze/`; validates bounds, routes malformed records to PostgreSQL `rejected_record`, upserts canonical `event`/`earthquake` and `event_group` tables, writes `TIMESTAMP(MICROS)` Parquet files to `datalake_silver/`, and triggers Gold (gated by `ShortCircuitOperator` cooldown).
3. **`gold_processing_dag`** (`0 */6 * * *`): Uses **PySpark** (`SparkSession.builder.master("local[*]")`) to read the entire `datalake_silver/` folder (`spark.read.parquet("datalake_silver/")`) and persist two Gold summary datasets in `datalake_gold/`:
   - `datalake_gold/governance_YYYYMMDD_HHMMSS.parquet` (structural health KPIs, null/pinned-depth rates, volume metrics, duplicate/match rates, schema compliance, IQR outlier rates, and `place` text length statistics).
   - `datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet` (Gutenberg-Richter severity & energy distributions, daily SAAI anomaly scores, top active regions, multi-agency comparisons, historical vs. current cohorts, focal depth regimes, and recent significant events).

You can also trigger the PySpark Gold pipeline directly from the CLI:
```bash
python -m src.gold_pyspark
```

### 5. Analytics & Monitoring Dashboards (Workshop #4 — Plotly Dash)

Workshop #4 delivers **two independent Plotly Dash applications** under `dashboard/`, decoupled from upstream ingestion and databases. Both applications **consume exclusively the Gold layer Parquet files** (`datalake_gold/governance_*.parquet` and `datalake_gold/storytelling_*.parquet`) via [`dashboard/gold_loader.py`](dashboard/gold_loader.py), strictly enforcing [`docs/gold_contract_w4.md`](docs/gold_contract_w4.md) (no direct reads from `datalake_silver/`, `datalake_bronze/`, HTTP APIs, or PostgreSQL).

- **Data Governance Dashboard ([`dashboard/governance_app.py`](dashboard/governance_app.py) — `http://localhost:8050`):**
  - *Target Audience:* Data Engineers & Pipeline Maintainers.
  - *Capabilities:* Top-level structural health KPI cards (records read, deduplicated count, intra-lake duplicate rate, schema compliance, 10 km pinned-depth rate, quarantined records, cross-source matched pairs), horizontal null & pinned-depth rate bar chart, daily/cumulative volume over time, Tukey IQR outlier rate & quartile spread panel, `place` string length statistics by agency, and an interactive Data Quality Summary Table with agency/status filters.
- **Seismic Storytelling Dashboard ([`dashboard/storytelling_app.py`](dashboard/storytelling_app.py) — `http://localhost:8051`):**
  - *Target Audience:* Functional User (Chief Seismic Risk Officer, Dr. Elena Vance).
  - *Capabilities:* Gutenberg-Richter severity vs. radiated energy share grouped bar chart, 3-class focal depth attenuation breakdown, daily Seismic Activity Anomaly Index (SAAI) trend with policy thresholds (`1.0`, `2.0`, `3.5`), daily seismic volume & swarm peaks, top 15 active tectonic regions, multi-agency comparison (USGS, EMSC, GDACS) & Historical vs. Current cohort toggle, global interactive epicenter & focal depth map (`scatter_geo`), and dynamic plain-language narrative summaries ([`dashboard/narrative.py`](dashboard/narrative.py)).

Both dashboards start automatically with `docker compose up -d --build`, or can be launched locally from the Python virtual environment:
```bash
python -m dashboard.governance_app      # http://localhost:8050
python -m dashboard.storytelling_app    # http://localhost:8051
```

### 6. Exploratory & Validation Jupyter Notebooks
- [`notebooks/01_source_profiling.ipynb`](notebooks/01_source_profiling.ipynb): Workshop #1 multi-source profiling & data quality identification.
- [`notebooks/02_silver_data_exploration.ipynb`](notebooks/02_silver_data_exploration.ipynb): Workshop #2 Silver Parquet schema and statistical exploration.
- [`notebooks/03_silver_data_quality_findings.ipynb`](notebooks/03_silver_data_quality_findings.ipynb): Workshop #3 Silver data quality audit (nulls, pinned 10 km depths, IQR/Z-score outliers, text length distributions) justifying Governance KPIs.
- [`notebooks/04_gold_summaries_exploration.ipynb`](notebooks/04_gold_summaries_exploration.ipynb): Workshop #3 Gold Parquet inspection previewing both `governance_*.parquet` and `storytelling_*.parquet`.
- [`notebooks/05_workshop4_gold_integration_validation.ipynb`](notebooks/05_workshop4_gold_integration_validation.ipynb): Workshop #4 Gold contract & dashboard integration validation.
- [`notebooks/06_workshop4_dashboard_design_lab.ipynb`](notebooks/06_workshop4_dashboard_design_lab.ipynb): Workshop #4 visual prototyping & layout validation.
- [`notebooks/07_workshop4_narrative_claims_check.ipynb`](notebooks/07_workshop4_narrative_claims_check.ipynb): Workshop #4 quantitative verification of dynamic narrative claims.

---

## 10. Team & Academic Governance
- **Course:** Programación para Análisis de Datos (PDA) — Semester 2026-II
- **Team 1:** Natural Hazard Monitoring Systems — Earthquake Activity Monitoring
- **Team Members:** Juan Córdoba (20211020097), Angelo Ibáñez (20212020007), Mauricio Sánchez (20212020045)
- **Deliverables:**
  - **Workshop #1:** Hazard Definition, Source Validation, and Data Contract
  - **Workshop #2:** Technical Architecture, Docker Stack, Canonical Historical Ingestion, and Bronze/Silver Pipeline
  - **Workshop #3:** Incremental Watermarks, Cross-Source Event Matching (`event_group`), Silver-to-Gold PySpark Pipeline, and Governance/Storytelling KPIs
  - **Workshop #4:** Analytics & Monitoring Dashboards (`governance_app.py` & `storytelling_app.py`), Gold Layer Data Contract (`docs/gold_contract_w4.md`), and Validation Suite
