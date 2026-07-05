# ADF Pipeline Notes — v3 Payments API → Bronze
**Day 3 | Auto Watermark + Audit Table**

---

## Files to Paste into ADF

| File | What it is | Paste into |
|---|---|---|
| `ds_voltgrid_payments_src_v3.json` | REST source dataset | ADF → Datasets |
| `ds_bronze_payments_sink_v3.json` | ADLS Gen2 JSON sink dataset | ADF → Datasets |
| `pl_bronze_api_payments_v3.json` | Full pipeline — watermark auto-read from audit table | ADF → Pipelines |

**Paste order — datasets first, then pipeline:**
1. `ds_voltgrid_payments_src_v3` → Publish all
2. `ds_bronze_payments_sink_v3` → Publish all
3. `pl_bronze_api_payments_v3` → Publish all

> ADF Studio: Author → Dataset or Pipeline → `{ }` Code button (top right) → select all → delete → paste → OK → Publish all

---

## Databricks Notebooks — Upload Before Running

Two notebooks are called by ADF via DatabricksNotebook activities. Upload them before the first pipeline run.

| Notebook file | Upload to Databricks Workspace |
|---|---|
| `notebooks_source_blob_migration/nb_get_watermark.ipynb` | `/Shared/ev_intelligence/bronze/nb_get_watermark` |
| `notebooks_source_blob_migration/nb_write_audit.ipynb` | `/Shared/ev_intelligence/bronze/nb_write_audit` |

**How to upload:**
1. Databricks → left sidebar → **Workspace**
2. Navigate to `/Shared/` → create folders `ev_intelligence/bronze/` if they don't exist
3. Click `⋮` → **Import** → select the `.ipynb` file
4. Repeat for both notebooks

---

## What Changed from v2 → v3

| | v2 | v3 |
|---|---|---|
| Watermark input | Manual `p_watermark` parameter — enter on every incremental run | Automatic — read from `pipeline_audit` Delta table |
| Parameters | `p_load_type`, `p_watermark` | `p_load_type` only |
| Audit | None | Every run writes to `dbw_ev_intelligence_dev.default.pipeline_audit` |
| First incremental run | Must pass watermark manually | Uses `1900-01-01T00:00:00Z` as safe fallback |

---

## Pipeline Flow — v3

```
pl_bronze_api_payments_v3
│
│  Parameter: p_load_type  ("full" | "incremental")
│
├── act_get_username        Web Activity — Key Vault → voltgrid-username
├── act_get_password        Web Activity — Key Vault → voltgrid-password
├── act_api_login           Web Activity — POST /api/auth/login/ → token
├── act_set_token           SetVariable  — v_token = token
├── act_set_ingestion_date  SetVariable  — v_ingestion_date = today (yyyy-MM-dd)
│
├── act_get_watermark       DatabricksNotebook — nb_get_watermark
│     │   Receives: pipeline_name, load_type
│     │   full        → returns "1900-01-01T00:00:00Z"
│     │   incremental → reads MAX(watermark_value) from pipeline_audit
│     │                 where pipeline_name = 'pl_bronze_api_payments_v3'
│     │                   and status = 'succeeded'
│     └── returns watermark string via dbutils.notebook.exit()
│
├── act_set_watermark       SetVariable  — v_watermark = runOutput
│
├── act_get_total_pages     Web Activity — GET /api/db/payments/?page=1&updated_after={v_watermark}
│                                          reads pagination.total_pages
├── act_set_total_pages     SetVariable  — v_total_pages = total_pages
│
├── act_paginate            Until loop (exits when v_current_page > v_total_pages)
│     ├── act_copy_payments_page   Copy Activity
│     │     Source: ds_voltgrid_payments_src_v3
│     │             GET /api/db/payments/?page={n}&page_size=100&updated_after={v_watermark}
│     │             Authorization: Token {v_token}
│     │     Sink:   ds_bronze_payments_sink_v3
│     │             bronze/api/payments/raw/ingestion_date={v_ingestion_date}/page_{n}.json
│     ├── act_set_temp_page        SetVariable — v_temp_page = v_current_page + 1
│     └── act_increment_page       SetVariable — v_current_page = v_temp_page
│
├── act_set_status_success  SetVariable — v_status = "succeeded"  (on loop success)
├── act_set_status_failed   SetVariable — v_status = "failed"     (on loop failure)
│
└── act_write_audit         DatabricksNotebook — nb_write_audit  ← ALWAYS runs
      Receives: pipeline_name, load_type, watermark_value, ingestion_date,
                total_pages, status, pipeline_run_id
      Writes 1 row to: dbw_ev_intelligence_dev.default.pipeline_audit
```

---

## Audit Table

**Table:** `dbw_ev_intelligence_dev.default.pipeline_audit`
**Type:** Managed Delta table (created automatically on first run by `nb_write_audit`)
**Location:** Inside `default` schema of the `dbw_ev_intelligence_dev` catalog

**Schema:**

| Column | Type | Description |
|---|---|---|
| `pipeline_name` | STRING | `pl_bronze_api_payments_v3` |
| `load_type` | STRING | `full` or `incremental` |
| `watermark_value` | STRING | The `updated_after` value used this run — next incremental reads this |
| `ingestion_date` | STRING | Bronze partition date written (`yyyy-MM-dd`) |
| `total_pages` | INT | Total pages fetched this run |
| `status` | STRING | `succeeded` or `failed` |
| `pipeline_run_id` | STRING | ADF RunId GUID — links to ADF Monitor |
| `run_timestamp` | TIMESTAMP | UTC time this row was written |

**Query audit history from Databricks:**
```sql
SELECT
    load_type,
    watermark_value,
    ingestion_date,
    total_pages,
    status,
    run_timestamp
FROM dbw_ev_intelligence_dev.default.pipeline_audit
WHERE pipeline_name = 'pl_bronze_api_payments_v3'
ORDER BY run_timestamp DESC
LIMIT 20;
```

---

## How Incremental Load Works Run by Run

```
Run 1 — Full load
  p_load_type = full
  nb_get_watermark → returns "1900-01-01T00:00:00Z"
  API fetches all records (no date filter)
  nb_write_audit → watermark_value = "1900-01-01T00:00:00Z", status = succeeded

Run 2 — Incremental
  p_load_type = incremental
  nb_get_watermark → reads audit → MAX(watermark_value) = "1900-01-01T00:00:00Z"
  API fetches records updated_after 1900 (still everything on second run)
  nb_write_audit → watermark_value = "1900-01-01T00:00:00Z", status = succeeded

  *** In production: after Run 1, manually update the audit row's watermark_value
      to MAX(updated_at) from the Bronze data, OR use a post-run Databricks job
      to compute and update it. Day 8 automates this fully. ***

Run N — Incremental (once watermark is properly set)
  p_load_type = incremental
  nb_get_watermark → reads audit → watermark = "2026-07-04T09:43:00Z"
  API fetches only records updated after that timestamp
  nb_write_audit → watermark_value = "2026-07-04T09:43:00Z", status = succeeded
```

---

## Trigger Setup

### First run — Full load
| Parameter | Value |
|---|---|
| `p_load_type` | `full` |

### Daily scheduled run — Incremental
| Parameter | Value |
|---|---|
| `p_load_type` | `incremental` |

```
ADF Studio → pl_bronze_api_payments_v3 → Add trigger → New/Edit
  Type       : Schedule
  Recurrence : Every 1 Day at 01:00 UTC
  Parameters : p_load_type = incremental
```

---

## Linked Services Required

| Linked Service | Used by |
|---|---|
| `ls_keyvault` | Referenced in KV Web Activities |
| `ls_voltgrid_api` | Source dataset base URL |
| `ls_adls_bronze` | Sink dataset — writes to `evdatalakedev` |
| `ls_databricks` | `act_get_watermark` and `act_write_audit` notebook activities |

`ls_databricks` must be created in ADF if not already present:
- ADF Studio → Manage → Linked services → + New → Azure Databricks
- Authentication: Managed Identity (recommended) or PAT stored in Key Vault

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_watermark` fails | `ls_databricks` not created | Create Databricks linked service in ADF Manage tab |
| `act_get_watermark` fails | Notebook not uploaded to correct path | Upload to `/Shared/ev_intelligence/bronze/nb_get_watermark` exactly |
| `act_write_audit` fails | `ls_databricks` cluster not running | Use job cluster or ensure cluster is running before trigger |
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` role | Portal → Key Vault → IAM → assign role to ADF MI, wait 2 min |
| `act_api_login` 401 | Wrong credentials in Key Vault | Check `voltgrid-username` and `voltgrid-password` values |
| Until loop runs only once | `v_total_pages` stayed at 1 | Check `act_get_total_pages` output in Monitor → confirm `pagination.total_pages` key exists |
| `act_copy_payments_page` 403 | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Portal → Storage → IAM → assign role |
| Incremental fetches all records every run | Watermark in audit table is `1900-01-01T00:00:00Z` | Update `watermark_value` in audit table to `MAX(updated_at)` from Bronze data |
