# Gold Layer Data Contract — Workshop #4 (`docs/gold_contract_w4.md`)

**Project:** Natural Hazard Monitoring Systems — **Team 1: Earthquake Activity Monitoring**  
**Course:** Data Analysis Programming (Semester 2026-II)  
**Producer:** `src/gold_pyspark.py` / `airflow/dags/gold_processing_dag.py`  
**Consumers:** `dashboard/governance_app.py` (`:8050`), `dashboard/storytelling_app.py` (`:8051`), `dashboard/gold_loader.py`, `scripts/validate_gold_contract.py`, `tests/test_workshop4.py`, and Notebooks `04`–`07`.

---

## 1. File Discovery & Versioning Rules

1. **Storage Directory:** `datalake_gold/` (overridable via `GOLD_DIR` environment variable for testing).
2. **File Naming Convention:**
   - Governance dataset: `governance_YYYYMMDD_HHMMSS.parquet`
   - Storytelling dataset: `storytelling_YYYYMMDD_HHMMSS.parquet`
3. **Latest-Snapshot Resolution:** Consumers **must** select the latest file of each kind by parsing and sorting the lexicographical UTC timestamp token `YYYYMMDD_HHMMSS` in the filename (never by filesystem `mtime`, which is altered by `git clone` or file copies).
4. **Exclusive Access Rule (R1):** Presentation modules under `dashboard/` read **only** these two Gold Parquet families via `pandas.read_parquet()`. Direct reads from `datalake_silver/`, `datalake_bronze/`, raw JSON/XML samples, HTTP APIs, or PostgreSQL are strictly prohibited in `dashboard/`.

---

## 2. Dataset 1 — `governance_YYYYMMDD_HHMMSS.parquet`

### 2.1 Column Schema (13 Columns)

| Column | Parquet / Pandas Type | Nullable | Semantics & Allowed Domain |
|---|---|:---:|---|
| `kpi_category` | `string` (`str`) | No | KPI family identifier (exact 6 categories below). |
| `metric_name` | `string` (`str`) | No | Canonical KPI identifier within `kpi_category`. |
| `source_id` | `string` (`str`) | No | `"ALL"` or source key (`usgs_fdsn`, `emsc_fdsn`, `usgs_realtime`, `gdacs_realtime`). |
| `period_date` | `string` (`str`) | No | `"ALL"` or UTC date `"YYYY-MM-DD"`. |
| `metric_value` | `float64` | **Yes** | Primary KPI value (`NaN` only when `compliance_status == "UNAVAILABLE"`). |
| `secondary_value` | `float64` | **Yes** | Supporting count, percentage share, or paired statistic (`NaN` if `UNAVAILABLE`). |
| `min_value` | `float64` | **Yes** | Minimum or $Q_1$ quantile (`25th` percentile for `outlier_rate_per_numeric_field`). |
| `median_value` | `float64` | **Yes** | Median ($Q_2$) or primary value echo. |
| `max_value` | `float64` | **Yes** | Maximum or $Q_3$ quantile (`75th` percentile for `outlier_rate_per_numeric_field`). |
| `threshold_value` | `float64` | **Yes** | Policy SLA threshold against which `compliance_status` is evaluated. |
| `compliance_status` | `string` (`str`) | No | `"PASS"`, `"WARN"`, or `"UNAVAILABLE"` (when external audit store is offline). |
| `unit` | `string` (`str`) | No | `"percent"`, `"records"`, `"characters"`, `"pairs"`, `"groups"`, or `"flag"`. |
| `computed_at` | `string` (`str`) | No | UTC generation timestamp `"YYYY-MM-DDTHH:MM:SSZ"`. |

### 2.2 Categories & Dashboard Component Mapping (`governance_app.py` on `:8050`)

| `kpi_category` | Expected Rows | Key Metrics / Meaning | Consuming Visual Component (`:8050`) |
|---|:---:|---|---|
| `summary` | `11` | `total_silver_records_read`, `unique_deduplicated_events`, `intra_lake_duplicate_rate_pct`, `schema_compliance_rate_pct`, `pinned_10km_depth_rate_pct`, `quarantined_records_detected` (`NaN`/`UNAVAILABLE` if DB offline), `quarantine_source_status`, `cross_source_matched_pairs`, `event_groups_total`, `event_group_size_2_count`, `event_group_size_3plus_count`. | Top KPI Summary Cards (7 cards) & Data Quality Summary Table |
| `null_rate_per_field` | `13` | Explicit null/empty % across all 12 core Silver columns (`threshold=5.0%`) + `depth_km_pinned_default_10km` (`threshold=25.0%`). | Horizontal Null & Pinned-Depth Rate Bar Chart + Summary Table |
| `volume_by_source` | `4` | Deduplicated record count (`metric_value`) and corpus share % (`secondary_value`) per seismic source. | Source Volume Filter Context & Summary Table |
| `volume_over_time` | `>= 1` | Daily ingested unique event count (`metric_value`) per `(period_date, source_id)` and running total (`secondary_value`). | Interactive Daily / Cumulative Volume Over Time Chart |
| `outlier_rate_per_numeric_field` | `4` | Tukey IQR outlier rate % (`metric_value`), outlier count (`secondary_value`), and $(Q_1, \text{median}, Q_3)$ in `(min_value, median_value, max_value)` for `magnitude`, `depth_km`, `log10_energy`, `energy_joules` (`threshold=15.0%`). | Outlier Rate & Quartile Spread Panel + Summary Table |
| `text_length_statistics` | `4` | Mean (`metric_value`), min, median, and max character length of `place` descriptor per source (`threshold=120` chars). | Place String Length Distribution by Agency Chart + Summary Table |

---

## 3. Dataset 2 — `storytelling_YYYYMMDD_HHMMSS.parquet`

### 3.1 Column Schema (25 Columns: 14 Core W3 + 11 Additive Nullable W4)

| # | Column | Parquet / Pandas Type | Nullable | Semantics & Validation Bounds |
|---:|---|---|:---:|---|
| 1 | `aggregation_type` | `string` (`str`) | No | Analytical slice family (exact 7 categories below). |
| 2 | `dimension_key` | `string` (`str`) | No | Primary grouping key (severity class, date, region, source, depth regime, `event_id`, or cohort name). |
| 3 | `secondary_dimension` | `string` (`str`) | No | Secondary label (`alert_tier`, severity class, human-readable place, window tag, or `"ALL"`). |
| 4 | `period_date` | `string` (`str`) | No | `"ALL"` or UTC date `"YYYY-MM-DD"`. |
| 5 | `event_count` | `int64` | No | Integer count of unique earthquakes in slice ($\ge 0$). |
| 6 | `percentage` | `float64` | No | Share of total deduplicated corpus ($[0, 100]$). |
| 7 | `mean_magnitude` | `float64` | No | Mean earthquake magnitude ($[-1.0, 10.0]$, or `0.0` if `event_count == 0`). |
| 8 | `max_magnitude` | `float64` | No | Peak earthquake magnitude ($[-1.0, 10.0]$, or `0.0` if `event_count == 0`). |
| 9 | `mean_depth_km` | `float64` | No | Mean hypocentral depth in km ($[-5.0, 800.0]$). |
| 10 | `log10_energy` | `float64` | No | $\log_{10}$ radiated seismic energy in Joules ($E = 10^{4.8 + 1.5M}$). |
| 11 | `saai_score` | `float64` | No | Daily Seismic Activity Anomaly Index ($0.4 Z_N + 0.6 Z_E$). |
| 12 | `alert_tier` | `string` (`str`) | No | `"NORMAL"`, `"ELEVATED"`, `"HIGH"`, `"CRITICAL"`, or `"INFO"`. |
| 13 | `narrative_note` | `string` (`str`) | No | Plain-language geophysical context, tsunami disclaimer, or coverage caveat. |
| 14 | `computed_at` | `string` (`str`) | No | UTC generation timestamp `"YYYY-MM-DDTHH:MM:SSZ"`. |
| 15 | `latitude` | `float64` | **Yes** | Epicenter latitude in decimal degrees ($[-90.0, 90.0]$); populated in `recent_significant_events`. |
| 16 | `longitude` | `float64` | **Yes** | Epicenter longitude in decimal degrees ($[-180.0, 180.0]$); populated in `recent_significant_events`. |
| 17 | `energy_share_pct` | `float64` | **Yes** | Percentage of total corpus radiated energy ($[0, 100]$); sums to `100.0%` across `severity_distribution`. |
| 18 | `event_timestamp_utc` | `string` (`str`) | **Yes** | Exact origin time `"YYYY-MM-DDTHH:MM:SSZ"` in `recent_significant_events`. |
| 19 | `source_id` | `string` (`str`) | **Yes** | Originating seismic agency in `recent_significant_events` and `source_comparison`. |
| 20 | `tsunami_flag` | `bool` / `object` | **Yes** | `True`/`False` for USGS catalogs (`usgs_fdsn`, `usgs_realtime`); `None`/`NaN` for agencies without USGS bulletin flags. |
| 21 | `is_pinned_depth` | `bool` / `object` | **Yes** | `True` if focal depth equals default `10.0 km` ($\pm 10^{-4}$) in `recent_significant_events`. |
| 22 | `daily_rate` | `float64` | **Yes** | Events per day in `historical_vs_current` and `temporal_trend_saai`. |
| 23 | `median_depth_km` | `float64` | **Yes** | Median hypocentral depth (km) in `historical_vs_current`. |
| 24 | `daily_energy_joules` | `float64` | **Yes** | Daily radiated seismic energy (J/day) in `historical_vs_current` and `temporal_trend_saai`. |
| 25 | `m55_plus_share_pct` | `float64` | **Yes** | Percentage of cohort events with $M \ge 5.5$ in `historical_vs_current`. |

### 3.2 Aggregation Families & Dashboard Component Mapping (`storytelling_app.py` on `:8051`)

| `aggregation_type` | Rows | User Story | Consuming Panel (`:8051`) |
|---|:---:|---|---|
| `severity_distribution` | `5` | `US-01` | **Panel A (Left):** Grouped Bar Chart (% of Events vs. % of Radiated Energy across 5 Gutenberg-Richter classes) + Narrative Summary Card |
| `depth_regime_breakdown` | `3` | `US-05`, `US-06` | **Panel A (Right):** 3-Class Focal Depth Attenuation Donut/Bar Chart (Shallow `0–70 km`, Intermediate `70–300 km`, Deep `>300 km`) |
| `temporal_trend_saai` | `>= 1` | `US-03`, `US-01` | **Panel B:** Daily SAAI Anomaly & Radiated Energy Trend Line with Policy Thresholds (`1.0`, `2.0`, `3.5`) + **Panel C:** Daily Seismic Volume & Swarm Peak Activity Chart + Narrative Card |
| `top_regions_keywords` | `<= 15` | `US-05` | **Panel D:** Top 15 Active Tectonic Regions Horizontal Bar Chart (colored & badged by `alert_tier`) |
| `source_comparison` | `>= 4` | `US-02` | **Panel E (Mode 1 — By Agency):** Multi-Agency Severity & Volume Breakdown |
| `historical_vs_current` | `4` | `US-02`, `US-01` | **Panel E (Mode 2 — Historical vs. Current):** Archival Baseline (`usgs_fdsn` + `emsc_fdsn`) vs. Rolling Real-Time Windows (`current_7d`, `current_24h`, `incremental_active_window`) with coverage caveats |
| `recent_significant_events` | `1..250` | `US-05` | **Panel F:** Global Interactive Epicenter & Hypocentral Depth Map (`scatter_geo`, size = magnitude, color = depth km, hover = UTC time, place, agency, tsunami notice) |

---

## 4. Invariant Validation Rules (`scripts/validate_gold_contract.py`)

1. **Completeness of Categories:** All 6 `kpi_category` values in Governance and all 7 `aggregation_type` values in Storytelling must exist and contain $\ge 1$ row.
2. **Cross-Dataset Volume Conservation:**
   - `summary.unique_deduplicated_events == sum(volume_by_source.metric_value)`
   - `summary.unique_deduplicated_events == sum(severity_distribution.event_count)`
   - `summary.unique_deduplicated_events == sum(depth_regime_breakdown.event_count)`
3. **Energy Conservation:** `abs(sum(severity_distribution.energy_share_pct) - 100.0) <= 0.1`.
4. **Physical & Geospatial Bounds on `recent_significant_events`:**
   - Zero nulls in `latitude`, `longitude`, `event_timestamp_utc`, `source_id`.
   - `latitude` $\in [-90.0, 90.0]$, `longitude` $\in [-180.0, 180.0]$, `mean_magnitude` $\in [-1.0, 10.0]$, `mean_depth_km` $\in [-5.0, 800.0]$.
   - `tsunami_flag` is strictly boolean (`True`/`False`) when `source_id` starts with `"usgs_"` and null (`NaN`/`None`) for non-USGS sources.
5. **Quarantine Status Integrity:** If `quarantine_source_status == 1.0`, `quarantined_records_detected` is non-null ($\ge 0$) and `compliance_status` $\in \{\text{PASS}, \text{WARN}\}$; if `quarantine_source_status == 0.0`, `quarantined_records_detected` is `NaN` and `compliance_status == "UNAVAILABLE"`.
