# Day 7 — ADF v4 Pipeline Replicated in Databricks
**`01_bronze_api_ingest_databricks.ipynb` — All 17 entities, same logic as ADF**

---

## What This Does

Replicates the full ADF metadata-driven pipeline (`pl_bronze_api_master_v4` + `pl_bronze_api_ingest_v4`) in a single Databricks notebook using Python + `requests` + `azure-storage-file-datalake`.

No ADF required. Same Bronze output, same watermark files, same audit CSV.

---

## ADF → Databricks: Side-by-Side Comparison

| ADF Component | Databricks Equivalent |
|---|---|
| `pl_bronze_api_master_v4` (master pipeline) | Cell 9 — `ThreadPoolExecutor(max_workers=17)` |
| `pl_bronze_api_ingest_v4` (child pipeline) | `ingest_entity()` function in Cell 8 |
| `act_read_metadata` Lookup | Cell 6 — `adls_read_text(CONFIG_PATH)` → `json.loads()` |
| `act_get_username / act_get_password` | Cell 7 — `dbutils.secrets.get(scope, key)` |
| `act_api_login` POST WebActivity | Cell 7 — `requests.post(AUTH_ENDPOINT, json=payload)` |
| `act_set_token` SetVariable | Cell 7 — `TOKEN = resp.json()["token"]` |
| `act_set_ingestion_date` SetVariable | Cell 9 — `datetime.now(UTC).strftime('%Y-%m-%d')` |
| `act_get_watermark` Lookup (per-entity CSV) | `read_watermark(entity_name)` in Cell 5 |
| `act_set_watermark` (full=epoch, else CSV) | `EPOCH_WATERMARK if LOAD_TYPE=="full" else csv_value` |
| `act_get_total_pages` GET WebActivity | GET page 1, read `pagination.total_pages` |
| `act_paginate` Until loop | `for page in range(1, total_pages + 1)` |
| `act_copy_entity_page` Copy Activity | `requests.get(url)` → `adls_write_bytes(path, data)` |
| `act_set_status_success / act_set_status_failed` | `status = "succeeded"` or `"failed"` in try/except |
| `act_write_audit` (always runs) | `adls_append_csv_row(AUDIT_CSV_PATH, row)` in `finally` |
| `act_write_watermark` (success only) | `write_watermark()` — only called when `status == "succeeded"` |
| `ForEach isSequential: false, batchCount: 20` | `ThreadPoolExecutor(max_workers=17)` |
| Linked service `ls_adls_bronze` (MSI) | `ClientSecretCredential` + `DataLakeServiceClient` |
| Key Vault WebActivity (MSI) | `dbutils.secrets.get(scope="kv-ev-scope", key=...)` |

---

## Bronze Output Structure (Identical to ADF)

```
abfss://bronze@evdatalakedev.dfs.core.windows.net/
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
| Secret `adls-tenant-id` in Key Vault | Day 1 — Azure AD tenant ID |
| Secret `adls-client-id` in Key Vault | Day 1 — Service Principal client ID |
| Secret `adls-client-secret` in Key Vault | Day 1 — Service Principal client secret |
| `pipeline_metadata_config.json` in ADLS | Day 5 Step 1a — uploaded to `bronze/config/` |
| Per-entity watermark CSVs in ADLS | Day 5 Step 1c — `bronze/audit/watermark_<entity>.csv` × 17 |
| `pipeline_audit.csv` in ADLS | Day 5 Step 1b — `bronze/audit/pipeline_audit.csv` |
| Service Principal has `Storage Blob Data Contributor` on `evdatalakedev` | Day 2 |

---

## Key Vault Secrets Reference

All secrets are in `kv-ev-intelligence-dev`, accessed via the `kv-ev-scope` Databricks secret scope.

| Secret Key | Value (what it holds) | Used in |
|---|---|---|
| `voltgrid-username` | VoltGrid API username | Cell 7 — API login |
| `voltgrid-password` | VoltGrid API password | Cell 7 — API login |
| `adls-tenant-id` | Azure AD tenant ID | Cell 4 — ADLS auth |
| `adls-client-id` | Service Principal client ID | Cell 4 — ADLS auth |
| `adls-client-secret` | Service Principal client secret | Cell 4 — ADLS auth |

---

## Cell-by-Cell Reference

| Cell | What it does | ADF equivalent |
|---|---|---|
| Cell 1 | Imports | — |
| Cell 2 | Reads `load_type` Job widget parameter | Pipeline parameter `p_load_type` |
| Cell 3 | Constants — API URL, ADLS account, paths | Linked service + hardcoded values |
| Cell 4 | Authenticates to ADLS via Service Principal | `ls_adls_bronze` (MSI) |
| Cell 5 | Helper functions — ADLS read/write, watermark read/write | Dataset operations |
| Cell 6 | Reads entity config JSON from ADLS | `act_read_metadata` Lookup |
| Cell 7 | Authenticates to VoltGrid API, gets token | `act_get_username/password` + `act_api_login` |
| Cell 8 | Defines `ingest_entity()` — full child pipeline logic | `pl_bronze_api_ingest_v4` |
| Cell 9 | Runs all entities in parallel, writes audit + watermarks | `act_foreach_entity` ForEach |
| Cell 10 | Summary table + raises on failure | ADF Monitor panel |

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

### Step 5 — Verify Cell 10 output

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

All 17 entities succeeded. Watermark files now contain today's UTC timestamp.

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

Each row = one run. Click into a row → scroll to Cell 10 output for the full summary.

### Healthy incremental run output

```
Cell 10:
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

Failed entity watermark is NOT advanced — next incremental run picks up from same point.

---

## Part E — Verify Bronze ADLS Contents

From any Databricks notebook:

```python
# Check entity data landed in Bronze
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

cred   = ClientSecretCredential(
    tenant_id     = dbutils.secrets.get("kv-ev-scope", "adls-tenant-id"),
    client_id     = dbutils.secrets.get("kv-ev-scope", "adls-client-id"),
    client_secret = dbutils.secrets.get("kv-ev-scope", "adls-client-secret")
)
client = DataLakeServiceClient("https://evdatalakedev.dfs.core.windows.net", credential=cred)
fs     = client.get_file_system_client("bronze")

# List all ingestion dates for payments
for item in fs.get_paths("api/payments"):
    print(item.name)

# Read a JSON page
fc = fs.get_file_client("api/payments/ingestion_date=2026-07-12/page_1.json")
data = json.loads(fc.download_file().readall())
print(f"payments page 1: {len(data['data'])} records")

# Check watermark files
for entity in ["payments", "sessions", "customers"]:
    fc  = fs.get_file_client(f"audit/watermark_{entity}.csv")
    wm  = fc.download_file().readall().decode()
    print(wm)
```

---

## Common Errors and Fixes

| Error | Cause | Fix |
|---|---|---|
| `Secret does not exist: voltgrid-username` | Secret missing from Key Vault or wrong name | Add `voltgrid-username` and `voltgrid-password` to Key Vault |
| `Secret does not exist: adls-client-id` | ADLS SP secrets missing | Add `adls-tenant-id`, `adls-client-id`, `adls-client-secret` to Key Vault |
| `Secret scope not found: kv-ev-scope` | Scope not created in Databricks | Create scope in Databricks Settings → Secrets |
| `401 Unauthorized` from API | Wrong username or password | Check `voltgrid-username`/`password` values in Key Vault |
| `ResourceNotFoundError` on config read | `pipeline_metadata_config.json` not in ADLS | Upload to `bronze/config/` — see Day 5 Step 1a |
| `ResourceNotFoundError` on watermark read | Watermark CSV missing | Run Day 5 Step 1c seed script, or set `load_type=full` (falls back to epoch) |
| `403 Forbidden` on ADLS write | SP lacks Storage Blob Data Contributor | Grant role on `evdatalakedev` in Azure Portal → IAM |
| All entities doing full load on incremental | Watermark files contain epoch `1900-01-01` | Check watermark files — run full load once to reset, or re-run seed script |
| `KeyError: pagination` from API | API response structure changed | Print `r1.json()` to inspect — check if `results` key used instead of `data` |
| Some entities timeout | API slow for large entities | Increase `timeout=60` in `requests.get()` call in Cell 8 |
| Audit CSV corrupted | Concurrent writes to shared file | Audit writes are sequential in Cell 9 (after all threads) — this should not happen |

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
| Auth to ADLS | Managed Identity (MSI) | Service Principal via Key Vault |
| Auth to Key Vault | MSI (no secret needed) | SP credentials → KV via SDK |
| JSON sink format | ADF Copy Activity `setOfObjects` | `json.dumps(page_data)` → raw bytes |
| Parallelism | ForEach `batchCount: 20` | `ThreadPoolExecutor(max_workers=17)` |
| Audit write | ADF Copy + additionalColumns | `csv.DictWriter` + `adls_append_csv_row` |
| Audit write timing | Activity-level dependsOn | Sequential after all threads join |
| Watermark write | ADF Copy + additionalColumns | `write_watermark()` string format |
| Error isolation | Activity failure sets v_status | try/except per entity in thread |
| Scheduling | ADF trigger | Databricks Job cron |
