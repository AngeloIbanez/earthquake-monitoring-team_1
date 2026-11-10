# Empirical Polling Evidence — USGS Real-Time Feed

**Team #1**
**Team members:**
- Juan Córdoba - 20211020097
- Mauricio Sánchez - 20212020045
- Angelo Ibáñez - 20212020007

## Methodology

To satisfy the Workshop #1 requirement for incremental source characterization, the USGS
real-time GeoJSON feed (`all_hour.geojson`) was polled **twice**, separated by $\ge 60$ minutes
(actual elapsed time: **78 minutes**). For each poll, every event ID, its magnitude, magnitude type,
place, and `properties.updated` epoch-ms timestamp were recorded.

Post-facto diff analysis systematically identifies:
1. **New events** that occurred and entered the rolling 1-hour window.
2. **Evicted events** that aged past the 60-minute window cutoff.
3. **In-place revised records** where downstream processing or additional seismic station telemetry
   triggered revisions to `updated`, `mag`, or alert products (e.g., DYFI, ShakeMap).

> **Watermark & Overlap Window Formalization:**
> The `properties.updated` field (Unix epoch ms) serves as the canonical high-water mark. Because USGS
> routinely updates preliminary automated earthquake locations with human-reviewed solutions,
> moment tensor inversions, and citizen felt-reports hours after the initial rupture, an **overlap window of 24 hours**
> is required. Each incremental run re-queries events updated within `[watermark - 24h, now()]` to capture late revisions
> using idempotent upsert operations (`ON CONFLICT (event_id) DO UPDATE`).

---

## Poll #1 Snapshot

| Field | Value |
|---|---|
| **Feed URL** | `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` |
| **Poll timestamp (UTC)** | `2026-11-09T05:33:48Z` |
| **Events in feed** | 12 |

### Event Snapshot — Poll #1

| Event ID | Magnitude | MagType | `properties.updated` (epoch ms) | Place |
|---|---|---|---|---|
| `hv75068727` | 1.83 | md | 1794201954680 | 3 km SSW of Pāhala, Hawaii |
| `hv75068722` | 2.08 | md | 1794201684110 | 3 km SSW of Pāhala, Hawaii |
| `aka2026werkhb` | 2.00 | ml | 1794201565958 | 43 km W of Karluk, Alaska |
| `hv75068707` | 2.23 | md | 1794200721450 | 7 km ENE of Pāhala, Hawaii |
| `us7000tx1i` | 4.60 | mb | 1794202279649 | 126 km WNW of Neiafu, Tonga |
| `aka2026weqwmc` | 1.40 | ml | 1794200550465 | 58 km SSW of Pedro Bay, Alaska |
| `hv75068702` | 2.01 | md | 1794200677230 | 7 km ENE of Pāhala, Hawaii |
| `hv75068697` | 1.70 | md | 1794200673180 | 7 km E of Pāhala, Hawaii |
| `us7000tx1f` | 5.70 | mww | 1794201646662 | 106 km ENE of Hasaki, Japan |
| `ci41360135` | 0.78 | ml | 1794200157579 | 6 km E of Ocotillo Wells, CA |
| `us7000tx1d` | 3.80 | mb | 1794200264040 | 66 km NNE of Barcelona, Venezuela |
| `nc75464292` | 0.95 | md | 1794199299292 | 8 km WNW of Cobb, CA |

---

## Poll #2 Snapshot

| Field | Value |
|---|---|
| **Feed URL** | `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` |
| **Poll timestamp (UTC)** | `2026-11-09T06:52:11Z` |
| **Elapsed time since Poll #1** | **78 minutes, 23 seconds** ($\ge 60\text{ min}$ rubric threshold met) |
| **Events in feed** | 10 |

### Event Snapshot — Poll #2

| Event ID | Magnitude | MagType | `properties.updated` (epoch ms) | Place |
|---|---|---|---|---|
| `nc75464332` | 1.08 | md | 1794206379533 | 4 km WNW of The Geysers, CA |
| `aka2026weucft` | 2.20 | ml | 1794206371536 | 29 km W of Anderson, Alaska |
| `ci41360151` | 0.61 | ml | 1794206365425 | 4 km WSW of Anza, CA |
| `aka2026wetpsh` | 1.40 | ml | 1794205495941 | 52 km SSE of Denali National Park, Alaska |
| `aka2026wetnhw` | 1.60 | ml | 1794205385835 | 58 km WNW of Ninilchik, Alaska |
| `nc75464327` | 1.05 | md | 1794205199044 | 9 km WNW of Cobb, CA |
| `aka2026wetcss` | 2.20 | ml | 1794204617476 | 40 km N of Petersville, Alaska |
| `aka2026wesydw` | 1.50 | ml | 1794204266032 | 71 km WSW of Karluk, Alaska |
| `ci41360143` | 0.72 | ml | 1794204074862 | 16 km NE of Borrego Springs, CA |
| `aka2026wesrtp` | 1.60 | ml | 1794203865451 | 42 km WSW of Tatitlek, Alaska |

---

## Diff Analysis

### 1. Rolling Window Eviction & Ingress ($|P_1 \cap P_2| = 0$)
Because the time between Poll #1 and Poll #2 (78 min) exceeded the 60-minute rolling duration of `all_hour.geojson`:
- **Evicted from window:** 12 events from Poll #1 are no longer present in the 1-hour feed.
- **New ingress events:** 10 new events arrived in the feed window between `05:52Z` and `06:52Z`.
- **Set Overlap:** $P_1 \cap P_2 = \emptyset$, empirically confirming that sliding-window feeds **drop events rapidly** and cannot serve as durable event storage without a persistent ingestion store.

### 2. Empirical Verification of In-Place Revisions
To test whether events that leave the rolling window receive revisions, all 12 events from Poll #1 were queried directly via the USGS FDSN Web Service at `2026-11-09T06:52:15Z`:

| Event ID | Poll #1 Mag | Poll #2 Mag | Poll #1 `updated` | Current `updated` | Empirical Status | Revision Nature |
|---|:---:|:---:|:---:|:---:|:---:|---|
| `hv75068727` | 1.83 | 1.83 | 1794201954680 | 1794201954680 | Unchanged | Baseline telemetry stable |
| `hv75068722` | 2.08 | 2.08 | 1794201684110 | 1794201684110 | Unchanged | Baseline telemetry stable |
| `aka2026werkhb` | 2.00 | 2.00 | 1794201565958 | **1794203052383** | **REVISED** | Seismic network phase pick refinement (+24.7 min later) |
| `hv75068707` | 2.23 | 2.23 | 1794200721450 | 1794200721450 | Unchanged | Baseline telemetry stable |
| `us7000tx1i` | 4.60 | 4.60 | 1794202279649 | 1794202279649 | Unchanged | Teleseismic preliminary solution |
| `aka2026weqwmc` | 1.40 | 1.40 | 1794200550465 | 1794200550465 | Unchanged | Baseline telemetry stable |
| `hv75068702` | 2.01 | 2.01 | 1794200677230 | 1794200677230 | Unchanged | Baseline telemetry stable |
| `hv75068697` | 1.70 | 1.70 | 1794200673180 | 1794200673180 | Unchanged | Baseline telemetry stable |
| `us7000tx1f` | 5.70 | 5.70 | 1794201646662 | **1794205392041** | **REVISED** | ShakeMap & DYFI ("Did You Feel It?") product ingestion (+62.4 min later) |
| `ci41360135` | 0.78 | 0.78 | 1794200157579 | 1794200157579 | Unchanged | Local network solution stable |
| `us7000tx1d` | 3.80 | 3.80 | 1794200264040 | 1794200264040 | Unchanged | Regional preliminary solution |
| `nc75464292` | 0.95 | 0.95 | 1794199299292 | 1794199299292 | Unchanged | Geysers geothermal network stable |

---

## Architectural Implications for Ingestion Design

| Design Requirement | Empirical Finding | Architectural Decision |
|---|---|---|
| **Watermark Target** | Origin time (`properties.time`) is static, but `properties.updated` changes when solutions are refined. | Track **`properties.updated`** as high-water mark; filtering on `time` alone misses all subsequent refinements. |
| **Overlap Window** | Events `us7000tx1f` and `aka2026werkhb` were revised 62 and 25 minutes after origin. Global events can be revised up to 24–48 hours later. | Ingest with **24-hour overlap window** (`updated >= watermark - 24 hours`) to continuously catch revised records. |
| **Idempotency Strategy** | The same `event_id` is retained across multiple revisions. | Apply **`ON CONFLICT (event_id) DO UPDATE SET severity = EXCLUDED.severity, ... updated_at = CURRENT_TIMESTAMP`** in PostgreSQL. |
| **Feed Selection** | `all_hour.geojson` has zero persistence across runs > 1h. | For production incremental ingestion, poll **`all_day.geojson`** or **`all_week.geojson`** every 5 minutes to prevent gaps during temporary pipeline downtime. |
| **Quarantine Rules** | Minor local events ($M < 1.0$) frequently have preliminary $md$ values with rapid station dropouts. | Enforce coordinate boundaries $[-90, 90], [-180, 180]$ and validate non-null magnitude before canonical loading; route malformed events to `rejected_record`. |
