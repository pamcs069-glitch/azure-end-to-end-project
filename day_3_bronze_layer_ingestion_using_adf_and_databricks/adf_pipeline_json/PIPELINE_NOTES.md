# Bronze Ingestion — ADF Pipeline Notes
**Day 3 | Payments API → Bronze Layer**

---

## Files in this folder

| File | What it is | Paste into |
|---|---|---|
| `ds_voltgrid_payments_src_v2.json` | REST source dataset | ADF → Datasets |
| `ds_bronze_payments_sink_v2.json` | ADLS Gen2 JSON sink dataset | ADF → Datasets |
| `pl_bronze_api_payments_v2.json` | Full pipeline with full/incremental load | ADF → Pipelines |

**Paste order — datasets first, then pipeline:**
1. `ds_voltgrid_payments_src_v2` → publish
2. `ds_bronze_payments_sink_v2` → publish
3. `pl_bronze_api_payments_v2` → publish

> In ADF Studio: Author → Dataset/Pipeline → `{ }` Code button (top right) → delete everything → paste → OK → Publish all

---

## Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                   pl_bronze_api_payments_v2                     │
│                                                                 │
│  PARAMETERS                                                     │
│  p_load_type : "full" or "incremental"                         │
│  p_watermark : "2026-07-04T09:43:00Z"  (incremental only)      │
│                                                                 │
│  act_get_username   ──► Key Vault → voltgrid-username           │
│         ↓                                                       │
│  act_get_password   ──► Key Vault → voltgrid-password           │
│         ↓                                                       │
│  act_api_login      ──► POST /api/auth/login/ → token           │
│         ↓                                                       │
│  act_set_token      ──► v_token = token                         │
│         ↓                                                       │
│  act_set_ingestion_date ──► v_ingestion_date = today            │
│         ↓                                                       │
│  act_set_watermark                                              │
│    full        → v_watermark = "1900-01-01T00:00:00Z"          │
│    incremental → v_watermark = p_watermark                      │
│         ↓                                                       │
│  act_get_total_pages ──► GET /api/db/payments/?page=1           │
│                              &updated_after={v_watermark}       │
│                          reads pagination.total_pages           │
│         ↓                                                       │
│  act_set_total_pages ──► v_total_pages = total_pages            │
│         ↓                                                       │
│  act_paginate  (Until: v_current_page > v_total_pages)          │
│    ├── act_copy_payments_page                                   │
│    │     GET /api/db/payments/?page={n}&updated_after={wm}      │
│    │     → bronze/api/payments/raw/ingestion_date={date}/       │
│    │         page_{n}.json                                      │
│    ├── act_set_temp_page   v_temp_page = v_current_page + 1     │
│    └── act_increment_page  v_current_page = v_temp_page         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Confirmed API Behaviour

All 18 VoltGrid endpoints accept `updated_after` as a query parameter:

```
GET /api/db/payments/?page=1&page_size=100&updated_after=2026-07-04T09:43:00Z
```

- Returns only records where `updated_at > updated_after` value
- URL-encode the timestamp when using in a browser: `2026-07-04T09%3A43%3A00Z`
- ADF handles encoding automatically — pass plain ISO 8601 in expressions
- Each payment record has: `created_at`, `updated_at`, `processed_at`

---

## Payment Record Fields

```json
{
  "id": 1343098,
  "payment_id": "PAY-AU-TRX-00839957",
  "session_id": "SESS-20250807-00839957",
  "customer_id": "CUST-AU-2025-001440",
  "gateway": "Square",
  "amount_aud": "486.69",
  "gst": "48.67",
  "payment_mode": "CreditCard",
  "status": "Failed",
  "processed_at": "2026-06-05T14:02:00Z",
  "created_at": "2026-07-04T14:02:37.670040Z",
  "updated_at": "2026-07-04T14:02:37.670049Z"
}
```

---

## Output Structure in ADLS

```
evdatalakedev
└── bronze/
    └── api/
        └── payments/
            └── raw/
                ├── ingestion_date=2026-07-04/   ← full load (day 1)
                │   ├── page_1.json
                │   ├── page_2.json
                │   └── page_N.json              ← N = total_pages
                │
                └── ingestion_date=2026-07-05/   ← incremental (day 2)
                    └── page_1.json              ← only changed records
```

---

## How to Trigger

### First run — Full load
| Parameter | Value |
|---|---|
| `p_load_type` | `full` |
| `p_watermark` | leave blank |

### Daily run — Incremental
| Parameter | Value |
|---|---|
| `p_load_type` | `incremental` |
| `p_watermark` | `2026-07-04T09:43:00Z` |

> `p_watermark` = `MAX(updated_at)` from previous run's Bronze data.
> Get it by running in Databricks after each pipeline run:

```python
from pyspark.sql.functions import explode, col, max as spark_max

df = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/ingestion_date=2026-07-05/"
)
payments = df.select(explode(col("data")).alias("p"))
max_wm = payments.select(spark_max(col("p.updated_at"))).collect()[0][0]
print(f"Use this as p_watermark next run: {max_wm}")
```

> Day 8 (Orchestration) will automate this — pipeline reads watermark from `pipeline_audit` Delta table automatically. For now, pass manually.

---

## Why v_temp_page exists (ADF self-reference limitation)

ADF does not allow a SetVariable to reference itself:
```
v_current_page = add(v_current_page, 1)   ← ERROR: self-reference
```

Workaround using a temp variable:
```
act_set_temp_page   : v_temp_page    = add(v_current_page, 1)
act_increment_page  : v_current_page = v_temp_page
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_get_username` 403 | ADF MI missing `Key Vault Secrets User` role | Portal → Key Vault → IAM → assign role to ADF MI |
| `act_api_login` 401 | Wrong credentials in Key Vault | Check `voltgrid-username` and `voltgrid-password` values |
| `act_get_total_pages` 401 | Token not set in `v_token` | Check `act_api_login` succeeded and `act_set_token` ran |
| Until loop runs only once | `v_total_pages` stayed at 1 | Check `act_get_total_pages` output → confirm `pagination.total_pages` exists |
| `act_copy_payments_page` 403 | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Portal → Storage → IAM → assign role |
| Incremental returns all records | `p_watermark` left blank on incremental run | Always pass watermark when `p_load_type=incremental` |
| `p_load_type` condition not working | Passed `Full` (capital F) instead of `full` | ADF `equals()` is case-sensitive — always lowercase |
