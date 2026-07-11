# Databricks Jobs Overview — Day 6 Bronze Blob Migration
**3 scheduled jobs, 3 notebooks, 3 cadences, 1 shared parameter**

---

## At a Glance

| Job | Notebook | Schedule | `load_type` (normal) | Data |
|---|---|---|---|---|
| `job_bronze_realtime_hourly` | `02_bronze_blob_all_entities_v2.ipynb` | Every hour `0 * * * *` | `incremental` | charging_sessions, maintenance_events CSVs |
| `job_bronze_invoices_daily` | `04_bronze_blob_invoices_pdf.ipynb` | Daily at 01:00 UTC `0 1 * * *` | `incremental` | PDF invoices |
| `job_bronze_reports_monthly` | `05_bronze_blob_reports_json.ipynb` | 1st of month 02:00 UTC `0 2 1 * *` | `incremental` | KPI / SLA / state breakdown JSONs |

---

## The `load_type` Parameter

Every Job passes **one parameter** to the notebook via a Databricks widget:

| Value | What the notebook does |
|---|---|
| `incremental` | Auto-computes the target window from `datetime.now(UTC)` — the right value for all scheduled runs |
| `full` | Copies all historical data regardless of what's already in Bronze — use once for first-time seeding |

**How it flows:**

```
Databricks Job
  └── Task parameter: load_type = "incremental"
        └── Notebook Cell 2:
              dbutils.widgets.text("load_type", "incremental", ...)
              load_type = dbutils.widgets.get("load_type")
```

The default widget value is `"incremental"` — safe to run the notebook interactively without setting the widget.

---

## First-Time Seeding (Before Scheduling)

Run each notebook **once manually** with `load_type = full` to copy all historical data:

1. Open `02_bronze_blob_all_entities_v2.ipynb` → set widget to `full` → Run All
2. Open `04_bronze_blob_invoices_pdf.ipynb` → set widget to `full` → Run All
3. Open `05_bronze_blob_reports_json.ipynb` → set widget to `full` → Run All
4. Create and activate the 3 Jobs (see individual guides)
5. Jobs will run on schedule with `load_type = incremental` from that point

---

## Individual Job Setup Guides

| File | Job |
|---|---|
| [JOB1_REALTIME_HOURLY.md](JOB1_REALTIME_HOURLY.md) | `job_bronze_realtime_hourly` — CSV files every hour |
| [JOB2_INVOICES_DAILY.md](JOB2_INVOICES_DAILY.md) | `job_bronze_invoices_daily` — PDFs every day |
| [JOB3_REPORTS_MONTHLY.md](JOB3_REPORTS_MONTHLY.md) | `job_bronze_reports_monthly` — JSONs every month |

---

## Upload All Notebooks First

Before creating any Job, upload all 3 scheduled notebooks to Databricks Workspace:

1. Databricks → left sidebar → **Workspace** → **Shared**
2. Click **⋮** → **Create** → **Folder** → name it `bronze_ingestion`
3. Inside `bronze_ingestion` → **⋮** → **Import** → import each notebook:
   - `02_bronze_blob_all_entities_v2.ipynb`
   - `04_bronze_blob_invoices_pdf.ipynb`
   - `05_bronze_blob_reports_json.ipynb`

Confirm paths:
```
/Shared/bronze_ingestion/02_bronze_blob_all_entities_v2
/Shared/bronze_ingestion/04_bronze_blob_invoices_pdf
/Shared/bronze_ingestion/05_bronze_blob_reports_json
```

---

## Monitoring All Jobs

Databricks → **Workflows** → filter by job name

| What to check | Where to look |
|---|---|
| Run succeeded / failed | Run history tab — green = OK, red = failed |
| Which files were copied | Click run → scroll to last cell output (run summary) |
| Email alerts | Notifications tab on each Job — add email for On Failure |
| Current Bronze contents | Any notebook: `dbutils.fs.ls("/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/")` |
