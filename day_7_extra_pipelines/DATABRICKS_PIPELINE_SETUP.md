# Day 7 — ADF v4 Pipeline Replicated in Databricks
**`01_bronze_api_ingest_databricks.ipynb` — All 17 entities, same logic as ADF**

---

## What This Does

Replicates the full ADF metadata-driven pipeline (`pl_bronze_api_master_v4` + `pl_bronze_api_ingest_v4`) in a single Databricks notebook using Python + `requests`.

No ADF required. Same Bronze output, same watermark files, same audit CSV.

**Storage access:** Unity Catalog Bronze Volume — `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/`
All file I/O uses `dbutils.fs.put` and Python `open()`. No ADLS credentials needed — Unity Catalog External Location + Storage Credential handles auth transparently.

---

## ADF → Databricks: Side-by-Side Comparison

| ADF Component | Databricks Equivalent |
|---|---|
| `pl_bronze_api_master_v4` (master pipeline) | Cell 8 — `ThreadPoolExecutor(max_workers=17)` |
| `pl_bronze_api_ingest_v4` (child pipeline) | `ingest_entity()` function in Cell 7 |
| `act_read_metadata` Lookup | Cell 5 — `volume_read_text(CONFIG_PATH)` → `json.loads()` |
| `act_get_username / act_get_password` | Cell 6 — `dbutils.secrets.get(scope, key)` |
| `act_api_login` POST WebActivity | Cell 6 — `requests.post(AUTH_ENDPOINT, json=payload)` |
| `act_set_token` SetVariable | Cell 6 — `TOKEN = resp.json()["token"]` |
| `act_set_ingestion_date` SetVariable | Cell 8 — `datetime.now(UTC).strftime('%Y-%m-%d')` |
| `act_get_watermark` Lookup (per-entity CSV) | `read_watermark(entity_name)` in Cell 4 |
| `act_set_watermark` (full=epoch, else CSV) | `EPOCH_WATERMARK if LOAD_TYPE=="full" else csv_value` |
| `act_get_total_pages` GET WebActivity | GET page 1, read `pagination.total_pages` |
| `act_paginate` Until loop | `for page in range(1, total_pages + 1)` |
| `act_copy_entity_page` Copy Activity | `requests.get(url)` → `volume_write_text(subpath, json)` |
| `act_set_status_success / act_set_status_failed` | `status = "succeeded"` or `"failed"` in try/except |
| `act_write_audit` (always runs) | `volume_append_csv_row(AUDIT_CSV_PATH, row)` — after threads join |
| `act_write_watermark` (success only) | `write_watermark()` — only when `status == "succeeded"` |
| `ForEach isSequential: false, batchCount: 20` | `ThreadPoolExecutor(max_workers=17)` |
| Linked service `ls_adls_bronze` (MSI) | Unity Catalog Volume — no credentials in code |
| Key Vault WebActivity (MSI) | `dbutils.secrets.get(scope="kv-ev-scope", key=...)` |

---

## Bronze Volume Output Structure (Identical to ADF)

```
/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/
  ├── config/
  │     └── pipeline_metadata_config.json          ← read-only (already uploaded)
  ├── audit/
  │     ├── pipeline_audit.csv                     ← append-only, 1 row per entity per run
  │     ├── watermark_payments.csv                 ← overwritten on success
  │     ├── watermark_sessions.csv
  │     └── watermark_<entity>.csv  × 17
  └── api/
        └── <entity>/
              └── ingestion_date=<YYYY-MM-DD>/
                    ├── page_1.json
                    ├── page_2.json
                    └── page_N.json
```

---

## Prerequisites

These must already be in place (all set up in previous days):

| Requirement | Where set up |
|---|---|
| Databricks secret scope `kv-ev-scope` | Day 1 — backed by `kv-ev-intelligence-dev` |
| Secret `voltgrid-username` in Key Vault | Day 1 — value: `voltgrid_demo` |
| Secret `voltgrid-password` in Key Vault | Day 1 — value: `EVcharge@AU2025` |
| Unity Catalog External Location on Bronze container | Day 1 / Day 2 — UC Storage Credential + External Location |
| `pipeline_metadata_config.json` in Bronze Volume | Day 5 Step 1a — `config/pipeline_metadata_config.json` |
| Per-entity watermark CSVs in Bronze Volume | Day 5 Step 1c — `audit/watermark_<entity>.csv` × 17 |
| `pipeline_audit.csv` in Bronze Volume | Day 5 Step 1b — `audit/pipeline_audit.csv` |

> **Note:** No Service Principal secrets (`adls-tenant-id`, `adls-client-id`, `adls-client-secret`) are needed. Storage access is via Unity Catalog — no credentials in the notebook code.

---

## Key Vault Secrets Reference

Only 2 secrets are needed — the API credentials:

| Secret Key | Value (what it holds) | Used in |
|---|---|---|
| `voltgrid-username` | VoltGrid API username | Cell 6 — API login |
| `voltgrid-password` | VoltGrid API password | Cell 6 — API login |

All secrets are in `kv-ev-intelligence-dev`, accessed via the `kv-ev-scope` Databricks secret scope.

---

## Cell-by-Cell Reference

| Cell | What it does | ADF equivalent |
|---|---|---|
| Cell 1 | Imports (`json`, `csv`, `requests`, `concurrent.futures`) | — |
| Cell 2 | Reads `load_type` Job widget parameter | Pipeline parameter `p_load_type` |
| Cell 3 | Constants — API URL, Volume path, audit/watermark paths | Linked service + hardcoded values |
| Cell 4 | Helper functions — Volume read/write, watermark read/write | Dataset operations |
| Cell 5 | Reads entity config JSON from Bronze Volume | `act_read_metadata` Lookup |
| Cell 6 | Authenticates to VoltGrid API, gets token | `act_get_username/password` + `act_api_login` |
| Cell 7 | Defines `ingest_entity()` — full child pipeline logic | `pl_bronze_api_ingest_v4` |
| Cell 8 | Runs all entities in parallel, writes audit + watermarks | `act_foreach_entity` ForEach |
| Cell 9 | Summary table + raises on failure | ADF Monitor panel |

---

## Part A — Upload the Notebook to Databricks

1. Databricks → left sidebar → **Workspace** → **Shared**
2. Open or create folder `bronze_ingestion`
3. **⋮** → **Import** → select `01_bronze_api_ingest_databricks.ipynb`

Confirm path:
```
/Shared/bronze_ingestion/01_bronze_api_ingest_databricks
```

---

## Part B — Run a Full Load (First Time)

Before scheduling, seed Bronze with all historical data.

### Step 1 — Open the notebook

Databricks → Workspace → `/Shared/bronze_ingestion/01_bronze_api_ingest_databricks`

### Step 2 — Set the widget

At the top of the notebook, a widget bar appears after Cell 2 runs.

Set `Load Type (full / incremental)` → **`full`**

Or set it before running by editing the widget default in Cell 2:
```python
dbutils.widgets.text("load_type", "full", ...)
```

### Step 3 — Attach to cluster

Click **Connect** → select `dev-cluster`

### Step 4 — Run All

Click **Run all** (top toolbar). Expected runtime: 5–30 minutes depending on entity sizes.

### Step 5 — Verify Cell 9 output

```
BRONZE API INGESTION — RUN SUMMARY
load_type      : full
ingestion_date : 2026-07-12
entities total : 17
succeeded      : 17
failed         : 0

  [OK] charge_cards          succeeded    3        1900-01-01T00:00:00Z
  [OK] chargers              succeeded    2        1900-01-01T00:00:00Z
  [OK] cities                succeeded    1        1900-01-01T00:00:00Z
  ...
  [OK] weather               succeeded    4        1900-01-01T00:00:00Z

Watermarks updated : 17 entity file(s)
```

---

## Part C — Create the Databricks Job

### Step 1 — Open Workflows

Databricks → left sidebar → **Workflows** → **+ Create job**

### Step 2 — Name the Job

Rename to:
```
job_bronze_api_ingest_databricks
```

### Step 3 — Configure the Task

| Field | Value |
|---|---|
| Task name | `task_api_ingest_all_entities` |
| Type | `Notebook` |
| Source | `Workspace` |
| Path | `/Shared/bronze_ingestion/01_bronze_api_ingest_databricks` |
| Cluster | `dev-cluster` (All-Purpose) |

### Step 4 — Add the `load_type` Parameter

Scroll to **Parameters** → **+ Add**:

| Key | Value |
|---|---|
| `load_type` | `incremental` |

> This maps directly to `dbutils.widgets.get("load_type")` in Cell 2.
> For the initial full load, use **Run now with different parameters** and set `load_type = full`.

### Step 5 — Set the Schedule

**Schedules & Triggers** → **+ Add schedule**:

| Field | Value |
|---|---|
| Trigger type | `Scheduled` |
| Schedule | `Custom cron` |
| Cron expression | `0 */2 * * *` |
| Timezone | `UTC` |

> `0 */2 * * *` = every 2 hours. Match this to how frequently the VoltGrid API publishes new data.
> Use `0 6 * * *` (once daily at 06:00 UTC) for lower-frequency needs.

### Step 6 — Email Alerts

**Notifications** → **On failure** → **+ Add notification** → enter your email.

### Step 7 — Save and Activate

**Save job** → toggle from **Paused** → **Active**.

---

## Part D — Monitor Runs

### View run history

Databricks → **Workflows** → `job_bronze_api_ingest_databricks` → **Run history** tab

Each row = one run. Click into a row → scroll to Cell 9 output for the full summary.

### Healthy incremental run output

```
Cell 9:
  load_type      : incremental
  ingestion_date : 2026-07-12
  entities total : 17
  succeeded      : 17
  failed         : 0

  [OK] payments    succeeded    1     2026-07-11T10:00:00Z
  [OK] sessions    succeeded    2     2026-07-11T10:00:00Z
  ...
```

### One entity failed (others still succeeded)

```
  [FAIL] complaints  failed    0     1900-01-01T00:00:00Z
         Error: HTTPSConnectionPool(...) Read timed out

  succeeded: 16  failed: 1
Exception: 1 entity(ies) failed: complaints — check output above.
Run status: Failed  ← email alert fires
```

Failed entity watermark is NOT advanced — next incremental run picks up from the same point.

---

## Part E — Verify Bronze Volume Contents

From any Databricks notebook:

```python
# Check entity data landed in Bronze Volume
BRONZE_VOLUME = "/Volumes/dbw_ev_intelligence_dev/default/bronze-volume"

# List all ingestion dates for payments
dates = dbutils.fs.ls(f"{BRONZE_VOLUME}/api/payments/")
for d in dates:
    print(d.path)

# Read a JSON page
with open(f"{BRONZE_VOLUME}/api/payments/ingestion_date=2026-07-12/page_1.json") as f:
    data = json.load(f)
print(f"payments page 1: {len(data['data'])} records")

# Check watermark files
for entity in ["payments", "sessions", "customers"]:
    with open(f"{BRONZE_VOLUME}/audit/watermark_{entity}.csv") as f:
        print(f.read())
```

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `Secret does not exist: voltgrid-username` | Secret missing or wrong name | Add `voltgrid-username` + `voltgrid-password` to Key Vault |
| `Secret scope not found: kv-ev-scope` | Scope not created | Create scope in Databricks Settings → Secrets |
| `401 Unauthorized` from VoltGrid API | Wrong username or password | Check values in Key Vault |
| `FileNotFoundError` on config read | `pipeline_metadata_config.json` missing from Volume | Upload to `config/` in Bronze Volume |
| `FileNotFoundError` on watermark read | Watermark CSV missing — notebook falls back to epoch | Run with `load_type=full` for first load; watermark created on success |
| `AnalysisException` / `Path does not exist` on Volume | UC External Location not set up | Verify Unity Catalog External Location points to Bronze container |
| All entities doing full load on incremental | Watermark files contain epoch | Run full load once; watermarks are set on success |
| `KeyError: pagination` from API | API response structure changed | Print `r1.json()` to inspect |
| Some entities timeout | API slow for large entity | Increase `timeout=60` in `requests.get()` in Cell 7 |
| Audit CSV missing header | First run creates it | Normal — `volume_append_csv_row` creates header automatically |

---

## Full Load vs Incremental Comparison

| | Full Load | Incremental |
|---|---|---|
| `load_type` param | `full` | `incremental` |
| Watermark sent to API | `1900-01-01T00:00:00Z` | Value from `watermark_<entity>.csv` |
| API returns | ALL records | Only records `updated_at > watermark` |
| Pages fetched | Many (hundreds for large entities) | Few (only delta since last run) |
| Runtime | 5–30 min | 1–5 min |
| When to use | First run, data recovery | Every scheduled run after |

---

## How This Differs from the ADF Pipeline

| Aspect | ADF v4 | Databricks notebook |
|---|---|---|
| Auth to ADLS | Managed Identity (MSI) | Unity Catalog External Location (transparent) |
| Auth to Key Vault | MSI (no secret needed) | Databricks secret scope `kv-ev-scope` |
| File I/O | ADF Copy Activity linked service | `dbutils.fs.put` + Python `open()` on Volume path |
| JSON sink format | ADF Copy Activity `setOfObjects` | `json.dumps(page_data)` → string |
| Parallelism | ForEach `batchCount: 20` | `ThreadPoolExecutor(max_workers=17)` |
| Audit write | ADF Copy + additionalColumns | `volume_append_csv_row` — sequential after threads join |
| Watermark write | ADF Copy + additionalColumns | `write_watermark()` — Volume path |
| Error isolation | Activity failure sets v_status | try/except per entity in thread |
| Scheduling | ADF trigger | Databricks Job cron |
