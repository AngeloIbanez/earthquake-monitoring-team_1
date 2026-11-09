# Canonical Data Mapping and Ingestion Contract

## 1. Canonical Event Model Mapping

Every raw record from the four sources is transformed into the standard canonical `event` model and extended in the `earthquake` domain table.

### Canonical Schema Definition (`event`)
- **`event_id`** (TEXT, PK): Unique system key (formatted as `{source_id}_{raw_id}`).
- **`source_id`** (TEXT, FK): Identifier of the originating data source (references `source(source_id)`).
- **`event_type`** (TEXT): Fixed value `"earthquake"`.
- **`timestamp`** (TIMESTAMPTZ): Event origin time in UTC (WGS84 timeline).
- **`latitude`** (DOUBLE PRECISION): Latitude coordinate in decimal degrees [-90.0 to 90.0].
- **`longitude`** (DOUBLE PRECISION): Longitude coordinate in decimal degrees [-180.0 to 180.0].
- **`severity`** (DOUBLE PRECISION): Quantitative measure of event magnitude.
- **`severity_unit`** (TEXT): Magnitude scale unit (e.g., `Mw`, `mb`, `ml`).
- **`raw_reference`** (TEXT): Original source URI or SHA-256 hash of the raw payload.
- **`updated_at`** (TIMESTAMPTZ): System synchronization timestamp (for incremental upserts and CDC).

### Domain Schema Definition (`earthquake`)
- **`event_id`** (TEXT, FK): References `event(event_id)`.
- **`depth_km`** (DOUBLE PRECISION): Hypocentrum depth in kilometers below sea level.
- **`magnitude_type`** (TEXT): Specific seismic magnitude measurement method (`mw`, `mb`, `ms`, `md`).
- **`place`** (TEXT): Geographic place description or Flynn-Region name.
- **`tsunami_flag`** (BOOLEAN): Tsunami evaluation flag (`true`/`false` if provided).

---

## 2. Field-Level Source Mapping Matrix

> **Note on GDACS source format (updated 2026-11-09):** The GDACS GeoJSON REST endpoint
> (`/gdacsapi/api/events/geteventlist/geojson`) returned HTTP 404 when validated. The
> operational feed is the **RSS 2.0 XML feed** at `https://www.gdacs.org/xml/rss_24h.xml`.
> All GDACS field mappings below refer to this XML format and its namespaces.
> Key namespaces: `gdacs=http://www.gdacs.org`, `georss=http://www.georss.org/georss`,
> `geo=http://www.w3.org/2003/01/geo/wgs84_pos#`.

| Target Canonical Field | USGS Historical (GeoJSON) | EMSC Historical (JSON) | USGS Incremental (GeoJSON) | GDACS Incremental (XML/RSS) | Transformation / Standardization Rule |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`event_id`** | `id` | `properties.source_id` or `properties.unid` | `id` | `{gdacs}eventid` text; prefix → `gdacs_{eventid}` | Prefix with source code (e.g., `usgs_us7000...`, `emsc_...`, `gdacs_...`) |
| **`timestamp`** | `properties.time` (epoch ms) | `properties.time` (ISO-8601 string) | `properties.time` (epoch ms) | `{gdacs}fromdate` (RFC-822 string) | Parse Epoch ms → `datetime.utcfromtimestamp(ms/1000)`; ISO-8601 / RFC-822 → `dateutil.parser.parse(..., tzinfos={'GMT': 0}).astimezone(UTC)` |
| **`latitude`** | `geometry.coordinates[1]` | `geometry.coordinates[1]` or `properties.lat` | `geometry.coordinates[1]` | `{georss}point` split by space → token[0] | Validate range [-90.0, 90.0]; quarantine if out-of-range |
| **`longitude`** | `geometry.coordinates[0]` | `geometry.coordinates[0]` or `properties.lon` | `geometry.coordinates[0]` | `{georss}point` split by space → token[1] | Validate range [-180.0, 180.0]; quarantine if out-of-range |
| **`severity`** | `properties.mag` (float) | `properties.mag` (float) | `properties.mag` (float) | Regex on `{gdacs}severity`: `r'Magnitude\s*([\d.]+)M'` | Convert to `DOUBLE PRECISION`; quarantine if `NaN` or `None` |
| **`severity_unit`** | `properties.magType` (string) | `properties.magtype` (string) | `properties.magType` (string) | Fixed literal `"mw"` (GDACS reports moment magnitude) | Lowercase-normalize all; allowed values: `mw`, `mb`, `ml`, `md`, `ms`, `mww`, `mwb`, `mwc`; default `"unk"` if absent |
| **`depth_km`** | `geometry.coordinates[2]` (float, km) | `geometry.coordinates[2]` or `properties.depth` (float, km) | `geometry.coordinates[2]` (float, km) | `null` — depth not present in RSS feed | Store as `DOUBLE PRECISION NULL`; set `null` for all GDACS records |
| **`place`** | `properties.place` (relative text) | `properties.flynn_region` (Flynn-Engdahl region name) | `properties.place` (relative text) | `title` element text (full sentence); strip prefix `"Green earthquake (...) in "` | Trim whitespace; default to `"Unknown"` if absent |
| **`tsunami_flag`** | `properties.tsunami` (integer 0/1) | _(not provided)_ → `false` | `properties.tsunami` (integer 0/1) | `{gdacs}calculationtype` == `"tsunami"` → `true`, else `false` | Cast int → boolean; default `false` |

---

## 3. Ingestion Contract Draft

### Historical Strategy
- **Partitioning:** Partition requests by month (`YYYY-MM`) to stay under the 20,000 record query limit imposed by the USGS FDSN service.
- **Idempotency:** Re-running historical ingestion overwrites existing records (`ON CONFLICT (event_id) DO UPDATE`), preventing duplicates.

### Incremental Strategy
- **USGS Real-time:** Polling every 5 minutes. Watermark field: `properties.updated` (Unix Epoch ms). Overlap window: 24 hours to capture revised magnitude/depth calculations.
- **GDACS Real-time:** Polling every 15 minutes. Watermark field: `todate` / `pubDate`. Overlap window: 48 hours.

### Failure Handling & Recovery
If an ingestion execution fails mid-process, the `ingestion_run` table records `status = "FAILED"`. The state watermark in `ingestion_state` is only updated upon 100% successful execution, ensuring data consistency.
