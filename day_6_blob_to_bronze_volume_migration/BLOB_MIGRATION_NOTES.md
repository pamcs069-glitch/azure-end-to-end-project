# Blob Migration Notes — Source Blob → Bronze Volume (All Folders)
**Day 6 | Source Blob (`dataenggdailystorage`) → Bronze Unity Catalog Volume**

---

## Source Blob Structure — What's Actually There

The instructor source blob (`source` container in `dataenggdailystorage`) has **4 top-level folders**, each with different data types, partition structures, and load cadences:

```
wasbs://source@dataenggdailystorage.blob.core.windows.net/
  ├── config/       19 XML files    — flat folder, static reference data (one-time load)
  ├── invoices/     ~450 PDF files  — YYYY/MM/DD/ partition, ~15 invoices/day
  ├── realtime/     ~68 CSV files   — YYYY/MM/DD/HH/ partition, 2 entities only
  └── reports/      3 JSON files    — YYYY/MM/ partition, 3 reports/month
```

> **Important:** `realtime/` has only 2 entities: `charging_sessions` and `maintenance_events`.
> Other EV entities (energy_prices, weather, sessions, payments, etc.) come from the VoltGrid API —
> handled by the ADF v4 pipeline in Day 5, not from this blob.

---

## Bronze Volume Target (mirrors source exactly)

```
/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/
  ├── config/       *.xml                          ← 19 XML files, flat
  ├── invoices/     YYYY/MM/DD/  INV-AU-*.pdf      ← ~450 PDFs, daily partitioned
  ├── realtime/
  │     ├── charging_sessions/   YYYY/MM/DD/HH/ *.csv
  │     └── maintenance_events/  YYYY/MM/DD/HH/ *.csv
  └── reports/      YYYY/MM/  *_YYYYMM.json        ← 3 JSONs per month
```

---

## Five Notebooks — One Per Data Flow

| Notebook | Source Folder | Type | Cadence |
|---|---|---|---|
| `01_bronze_blob_all_entities.ipynb` | `realtime/` | CSV | Manual (full or incremental by hour) |
| `02_bronze_blob_all_entities_v2.ipynb` | `realtime/` | CSV | Scheduled Job — hourly, 3-hour look-back |
| `03_bronze_blob_config_xml.ipynb` | `config/` | XML | One-time (re-run only when source changes) |
| `04_bronze_blob_invoices_pdf.ipynb` | `invoices/` | PDF | Manual (full or daily by date) |
| `05_bronze_blob_reports_json.ipynb` | `reports/` | JSON | Manual (full or monthly) |

---

## Source Detail: `realtime/` (Notebooks 01 + 02)

**Entities:** `charging_sessions`, `maintenance_events` only.

**Partition:** `YYYY/MM/DD/HH/` — one CSV per hour per entity.

**File naming — two variants exist:**

| Entity | Standard name | Variant name (Jun 19 & Jun 22 only) |
|---|---|---|
| charging_sessions | `sessions_YYYYMMDD_HHMM.csv` | `charging_sessions_YYYYMMDD_HHMM.csv` |
| maintenance_events | `maintenance_YYYYMMDD_HHMM.csv` | `maintenance_events_YYYYMMDD_HHMM.csv` |

> Jun 19 and Jun 22 have **2 CSVs in the same hour partition** (both naming variants). Both are copied
> to Bronze. The Silver layer deduplicates by primary key.

**Data range:** June 1 – June 30, 2026 + July 7, 2026 (hours 06, 13, 15)

### v1 (Manual) — `01_bronze_blob_all_entities.ipynb`

Change only Cell 2:
```python
LOAD_MODE = "full"        # first run
LOAD_MODE = "incremental" # subsequent runs — set LOAD_YEAR/MONTH/DAY/HOUR
```

### v2 (Scheduled) — `02_bronze_blob_all_entities_v2.ipynb`

3-hour look-back window auto-computed from `datetime.now(UTC)`. Two checks per entity-hour slot:
1. Already in Bronze? → SKIP
2. Source folder missing? → SKIP (retry on next run's window)

**Job:** `job_bronze_all_entities_hourly` | Cron: `0 * * * *` | Timezone: UTC

---

## Source Detail: `config/` (Notebook 03)

**19 XML files, flat folder — no date partitioning.**

| Files | Purpose |
|---|---|
| `connector_standards.xml`, `connector_types.xml` | EV connector specifications |
| `firmware_manifest_v7_4_2.xml`, `v7_5_0.xml`, `v8_0_1.xml` | Charger firmware manifests |
| `network_topology.xml` | Network layout |
| `operator_registry.xml` | Charging operator registry |
| `site_types.xml` | Site classification |
| `states.xml` | Australian state reference |
| `station_config_ACT/NSW/NT/QLD/SA/TAS/VIC/WA.xml` | Station config per state (8 files) |
| `tariffs.xml`, `tariff_rate_card_202606.xml` | Tariff definitions and June 2026 rate card |

**Load pattern:** One-time full copy. Re-run only when instructor updates config files.

**Silver use:** These files are loaded once and broadcast as lookup tables joined to realtime/API data.

**XML reading in Spark:**
```python
# Requires spark-xml package on cluster
df = spark.read.format("xml") \
    .option("rowTag", "Connector") \
    .load("/Volumes/.../bronze-volume/config/connector_types.xml")
```
Maven package: `com.databricks:spark-xml_2.12:0.18.0` (pre-installed on DBR 13+)

---

## Source Detail: `invoices/` (Notebook 04)

**~450 PDF files, partitioned by `YYYY/MM/DD/`.**

| Attribute | Value |
|---|---|
| Year/month | June 2026 only |
| Days | All 30 days of June |
| Files per day | 15 PDFs |
| Invoice ID pattern | `INV-AU-2026-NNNNNNNN` |
| File name pattern | `INV-AU-2026-0002266.pdf` |

**Load modes:**
```python
LOAD_MODE = "full"   # first run — all 30 days
LOAD_MODE = "daily"  # subsequent — set LOAD_YEAR/MONTH/DAY
```

**Bronze stores PDFs as-is.** The invoice ID and date are parseable from the file path alone — no PDF parsing needed for basic metadata.

**Silver use:** Two options depending on requirements:
- **Metadata-only:** Parse invoice ID and date from file path → Delta table with path reference
- **Content extraction:** Use a PDF parsing library (PyMuPDF / pdfminer) in a Silver notebook to extract invoice amount, customer ID, etc. from the PDF body

---

## Source Detail: `reports/` (Notebook 05)

**3 JSON files per month, partitioned by `YYYY/MM/`.**

| File | Content |
|---|---|
| `kpi_report_YYYYMM.json` | KPI metrics — uptime, utilisation, revenue, sessions count |
| `sla_report_YYYYMM.json` | SLA metrics — response times, breach counts, availability % |
| `state_breakdown_YYYYMM.json` | State-level aggregations — sessions/revenue/faults per AU state |

Current data: **June 2026 only** (3 files total).

**Load modes:**
```python
LOAD_MODE = "full"    # first run — all months
LOAD_MODE = "monthly" # new month — set LOAD_YEAR/LOAD_MONTH
```

**Silver use:** Flatten nested JSON fields → monthly aggregated Delta tables for Gold-layer dashboards.

---

## Prerequisites (All Notebooks)

| Requirement | Where to set it up |
|---|---|
| `kv-ev-scope` Databricks secret scope | Day 1 Part 6.5 |
| `source-storage-account` in Key Vault | `dataenggdailystorage` |
| `source-container` in Key Vault | `source` |
| `source-sas-token` in Key Vault | SAS with `sp=rl` (read + list), expiry 2027-07-30 |
| Bronze Volume exists | Day 2 — `05_UNITY_CATALOG_EXTERNAL_LOCATIONS.md` Part 5 |
| Cluster attached to Unity Catalog metastore | Databricks workspace setup |

---

## First-Time Setup Order

Run these once in order to populate Bronze with all historical data:

1. **`03_bronze_blob_config_xml.ipynb`** — run all cells (19 XML files, ~seconds)
2. **`01_bronze_blob_all_entities.ipynb`** — Cell 2: `LOAD_MODE = "full"`, run all cells
3. **`04_bronze_blob_invoices_pdf.ipynb`** — Cell 2: `LOAD_MODE = "full"`, run all cells (~450 PDFs)
4. **`05_bronze_blob_reports_json.ipynb`** — Cell 2: `LOAD_MODE = "full"`, run all cells (3 JSONs)
5. Create Databricks Job for **`02_bronze_blob_all_entities_v2.ipynb`** (hourly, cron `0 * * * *`)
6. For invoices and reports: create monthly scheduled jobs or run manually at month-end

---

## Databricks Job Setup (Realtime v2 — Hourly)

### Step 1 — Upload Notebooks

1. Databricks → **Workspace** → **Shared** → create folder `bronze_ingestion`
2. Import all 5 notebooks into the folder

### Step 2 — Create the Hourly Job (v2 only)

1. **Workflows** → **+ Create job**
2. Name: `job_bronze_all_entities_hourly`
3. Task → Type: `Notebook` → Path: `/Shared/bronze_ingestion/02_bronze_blob_all_entities_v2`
4. Cluster: `dev-cluster` (All-Purpose — already warm, no cold start)
5. **Schedules & Triggers** → `+ Add schedule` → Custom cron: `0 * * * *`, Timezone: `UTC`
6. **Notifications** → On failure → your email
7. **Save job** → toggle to **Active**

### Step 3 — Manual test

Databricks → Workflows → `job_bronze_all_entities_hourly` → **Run now** → check Cell 8 summary.

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `Secret does not exist` | Wrong key name in Key Vault | Check `source-storage-account`, `source-container`, `source-sas-token` |
| `403 Forbidden` on `dbutils.fs.ls` | SAS token expired or wrong permissions | Regenerate SAS with `sp=rl`, update Key Vault |
| `Path does not exist` on source | Entity folder / date folder missing | Check source blob in portal — source data not yet available |
| Bronze assertion fails | Partial copy | Check FAILED lines, fix permission, re-run |
| Job not firing | Job is Paused | Toggle to Active in Workflows |
| XML `rowTag` error | Wrong element name for spark-xml | Open XML file with `dbutils.fs.head` to see root element name |
| PDF content unreadable | Binary file — Spark cannot read PDF text natively | Use PyMuPDF in Silver notebook; Bronze stores raw PDFs only |
| Jun 19/Jun 22 duplicate files | Source has 2 CSVs per hour for these dates | Expected — both are copied. Silver deduplicates by primary key |

---

## Bronze Volume Path Reference

| Data | Bronze path |
|---|---|
| All config XML | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/config/` |
| charging_sessions (all) | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/` |
| charging_sessions (one hour) | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/06/01/06/` |
| maintenance_events (all) | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/maintenance_events/` |
| invoices (all) | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/invoices/` |
| invoices (one day) | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/invoices/2026/06/01/` |
| reports (June 2026) | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/reports/2026/06/` |

---

## What Comes Next

All source data now lands in Bronze as raw files. Silver layer (Day 7) reads from Bronze Volume and:

| Data | Silver treatment |
|---|---|
| `realtime/` CSVs | Apply explicit schema, deduplicate by session/event ID, write Delta |
| `config/` XMLs | Parse with spark-xml, broadcast as lookup DataFrames |
| `invoices/` PDFs | Parse file-path metadata → Delta; optionally extract PDF content |
| `reports/` JSONs | Flatten nested fields, write monthly aggregated Delta tables |
