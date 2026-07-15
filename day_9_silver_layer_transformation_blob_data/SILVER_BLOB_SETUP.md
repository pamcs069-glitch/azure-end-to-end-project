# Day 9 — Silver Layer: Blob Data Transformation

## All notebooks

### Part A — Realtime blob (CSV): charging_sessions + maintenance_events

| Notebook | Scope | Purpose |
|---|---|---|
| `01_silver_charging_sessions_simple_v1.ipynb` | charging_sessions only | Every step written explicitly — no functions, no loops. Full overwrite. |
| `02_silver_blob_all_entities_forloop_v2.ipynb` | charging_sessions + maintenance_events | Same logic wrapped in a for loop. Full overwrite. |
| `03_silver_blob_all_entities_job_params_v3.ipynb` | charging_sessions + maintenance_events | Production — job parameters only, data quality pipeline, Delta MERGE. |

**Teach in order: v1 → v2 → v3**

### Part B — Invoice metadata (Bronze Delta → Silver Delta)

| Notebook | Scope | Purpose |
|---|---|---|
| `04_silver_blob_invoices_v1.ipynb` | invoices only | Single entity, every step explicit. Full overwrite. |
| `05_silver_blob_invoices_forloop_v2.ipynb` | invoices (extensible to other batch entities) | Entity config list + for loop. Full overwrite. |
| `06_silver_blob_invoices_job_params_v3.ipynb` | invoices | Production — job parameters only, data quality, Delta MERGE. |

### Part C — Monthly reports (Bronze JSON → Silver Delta)

| Notebook | Scope | Purpose |
|---|---|---|
| `07_silver_blob_reports_v1.ipynb` | kpi_report only | Single report type, every step explicit. Full overwrite. |
| `08_silver_blob_reports_forloop_v2.ipynb` | kpi_report + sla_report + state_breakdown | Report config list + per-type flatten functions + for loop. Full overwrite. |
| `09_silver_blob_reports_job_params_v3.ipynb` | all 3 report types | Production — job parameters only, data quality, Delta MERGE. |

---

## Source → Silver data flow

```
Bronze Volume (CSV, hourly partitions)
  realtime/charging_sessions/YYYY/MM/DD/HH/sessions_YYYYMMDD_HHMM.csv
  realtime/maintenance_events/YYYY/MM/DD/HH/maintenance_YYYYMMDD_HHMM.csv

      PySpark reads CSV → cast types → data quality → dedup → Delta MERGE

Silver Volume (Delta tables)
  realtime/charging_sessions/    (Delta — MERGE upsert on session_id)
  realtime/maintenance_events/   (Delta — MERGE upsert on event_id)

Silver Volume (Quarantine)
  quarantine/realtime/charging_sessions/    (rejected rows with reject_reason)
  quarantine/realtime/maintenance_events/
```

---

## Key difference from Day 8 (API data)

| | Day 8 (API JSON) | Day 9 (Blob CSV) |
|---|---|---|
| Bronze format | JSON with `data[]` array wrapper | CSV with header row |
| Read step | `spark.read.json()` + `explode(data[])` | `spark.read.csv(header=true)` |
| Partition structure | `ingestion_date=YYYY-MM-DD/` (flat) | `YYYY/MM/DD/HH/` (hierarchical) |
| Job parameters | `load_type`, `ingestion_date` | `load_type`, `ingestion_year`, `ingestion_month`, `ingestion_day`, `ingestion_hour` |
| No explode needed | No — CSV is already flat rows |

---

## Part A — Upload v3 notebook to Databricks

1. Databricks → **Workspace** → **Shared** → `silver_transformation` folder
2. **Import** → select `03_silver_blob_all_entities_job_params_v3.ipynb`
3. Confirm path:
   ```
   /Shared/silver_transformation/03_silver_blob_all_entities_job_params_v3
   ```

---

## Part B — Attach to existing Databricks Job (job_bronze_realtime_hourly)

The notebook attaches as a **second task** in the existing `job_bronze_realtime_hourly` job so Silver runs automatically after Bronze completes each hour.

### Step 1 — Open the existing Bronze job

1. Databricks → left sidebar → **Workflows** → **Jobs**
2. Click `job_bronze_realtime_hourly`
3. Click **Edit** (or the task canvas area)

### Step 2 — Add Silver as a new task

1. On the task canvas, click **+ Add task** → **Notebook**
2. A new task box appears — connect it after the existing Bronze task with a dependency arrow

### Step 3 — Configure the Silver task

| Field | Value |
|---|---|
| Task name | `silver_blob_transform` |
| Type | Notebook |
| Source | Workspace |
| Path | `/Shared/silver_transformation/03_silver_blob_all_entities_job_params_v3` |
| Cluster | Select your `dev-cluster` |
| Depends on | `<your existing bronze task name>` |
| Timeout | 3600 seconds (1 hour) |
| Retries | 1 |

### Step 4 — Add task parameters

In the task configuration → **Parameters** section → click **+ Add**:

| Key | Value |
|---|---|
| `load_type` | `incremental` |
| `ingestion_year` | `{{job.start_time.iso_date \| date_format: 'yyyy'}}` |
| `ingestion_month` | `{{job.start_time.iso_date \| date_format: 'MM'}}` |
| `ingestion_day` | `{{job.start_time.iso_date \| date_format: 'dd'}}` |
| `ingestion_hour` | `{{job.start_time.iso_date \| date_format: 'HH'}}` |

> These dynamic values inject the job's scheduled run time as the partition to process. Bronze writes `YYYY/MM/DD/HH/` and Silver reads the same partition automatically.

### Step 5 — Save and verify job structure

After saving, the job task graph should look like:

```
job_bronze_realtime_hourly
  │
  ├── task: bronze_ingest    (existing — reads from blob, writes Bronze CSV)
  │
  └── task: silver_blob_transform    (NEW — reads Bronze CSV, writes Silver Delta)
        dependsOn: bronze_ingest [Success]
        Parameters:
          load_type       = incremental
          ingestion_year  = {{run_time.year}}
          ingestion_month = {{run_time.month}}
          ingestion_day   = {{run_time.day}}
          ingestion_hour  = {{run_time.hour}}
```

### Step 6 — Test run

1. Click **Run now** on the job
2. Monitor: Workflows → Job Runs → expand the run
   - Bronze task: Succeeded
   - Silver task: Succeeded
3. Verify Silver output:
   ```python
   SILVER_REALTIME = "/Volumes/dbw_ev_intelligence_dev/default/silver-volume/realtime"
   for entity in ["charging_sessions", "maintenance_events"]:
       df = spark.read.format("delta").load(f"{SILVER_REALTIME}/{entity}")
       print(f"{entity:<25} rows={df.count()}")
   ```

---

## Part C — Run Silver independently (backfill)

If you need to reprocess a specific hour without re-running Bronze:

1. Databricks → **Workflows** → open `job_bronze_realtime_hourly` → **Run now with different parameters**
2. Or trigger just the `silver_blob_transform` task
3. Set parameters:
   | Key | Value |
   |---|---|
   | `load_type` | `incremental` |
   | `ingestion_year` | `2026` |
   | `ingestion_month` | `07` |
   | `ingestion_day` | `15` |
   | `ingestion_hour` | `06` |

---

## Job schedule reference

| Job | Cron | What runs |
|---|---|---|
| `job_bronze_realtime_hourly` | `0 0 * * * ?` (every hour on the hour) | Bronze: blob CSV → Bronze Volume |
| Silver realtime (attached to above) | Runs after Bronze task succeeds | Silver: Bronze CSV → Silver Delta (charging_sessions, maintenance_events) |
| `job_silver_blob_invoices_daily` | `0 30 1 * * ?` (01:30 UTC daily) | Silver: invoice metadata Bronze → Silver Delta |
| `job_silver_blob_reports_monthly` | `0 0 2 2 * ?` (02:00 UTC on 2nd of each month) | Silver: all 3 JSON report types → Silver Delta |

---

## Part D — Invoice Silver setup

### Step 1 — Upload invoice v3 notebook
1. Databricks → **Workspace** → **Shared** → `silver_transformation`
2. **Import** → select `06_silver_blob_invoices_job_params_v3.ipynb`
3. Confirm path: `/Shared/silver_transformation/06_silver_blob_invoices_job_params_v3`

### Step 2 — Option A: Attach to existing invoice Bronze job

If you have `job_bronze_invoices_daily` already running (from Day 6):
1. Databricks → **Workflows** → **Jobs** → open `job_bronze_invoices_daily`
2. **+ Add task** → **Notebook**
3. Configure:

| Field | Value |
|---|---|
| Task name | `silver_invoices_transform` |
| Path | `/Shared/silver_transformation/06_silver_blob_invoices_job_params_v3` |
| Depends on | `<bronze invoice task name>` |

4. Parameters:

| Key | Value |
|---|---|
| `load_type` | `incremental` |
| `ingestion_year` | `{{job.start_time.iso_date \| date_format: 'yyyy'}}` |
| `ingestion_month` | `{{job.start_time.iso_date \| date_format: 'MM'}}` |
| `ingestion_day` | `{{job.start_time.iso_date \| date_format: 'dd'}}` |

### Step 2 — Option B: Create standalone invoice Silver job

Import `job_silver_blob_invoices_schedule.json` via Databricks Jobs API:
```bash
curl -X POST https://<your-workspace>.azuredatabricks.net/api/2.1/jobs/create \
  -H "Authorization: Bearer <token>" \
  -d @job_silver_blob_invoices_schedule.json
```

---

## Part E — Reports Silver setup

### Step 1 — Upload reports v3 notebook
1. Databricks → **Workspace** → **Shared** → `silver_transformation`
2. **Import** → select `09_silver_blob_reports_job_params_v3.ipynb`
3. Confirm path: `/Shared/silver_transformation/09_silver_blob_reports_job_params_v3`

### Step 2 — Create standalone reports Silver job

Reports are monthly so a standalone job makes more sense than attaching to Bronze (which runs daily).

Import `job_silver_blob_reports_schedule.json` via Databricks Jobs API:
```bash
curl -X POST https://<your-workspace>.azuredatabricks.net/api/2.1/jobs/create \
  -H "Authorization: Bearer <token>" \
  -d @job_silver_blob_reports_schedule.json
```

Or create manually via UI:
1. Databricks → **Workflows** → **+ Create job**
2. Name: `job_silver_blob_reports_monthly`
3. Schedule: **Monthly**, 2nd day of month, 02:00 UTC
4. Task → Notebook: `/Shared/silver_transformation/09_silver_blob_reports_job_params_v3`
5. Parameters:

| Key | Value |
|---|---|
| `load_type` | `incremental` |
| `ingestion_year` | `{{job.start_time.iso_date \| date_format: 'yyyy'}}` |
| `ingestion_month` | `{{job.start_time.iso_date \| date_format: 'MM'}}` |

> **Why 2nd of the month?** Bronze report files are written on the 1st (end-of-month reports).
> Running Silver on the 2nd gives Bronze time to complete and upload all 3 files.

---

## Silver Delta table reference

### Realtime (CSV)
| Entity | Natural Key | CDC Field | Silver Path |
|---|---|---|---|
| charging_sessions | `session_id` | `updated_at` | `.../realtime/charging_sessions/` |
| maintenance_events | `event_id` | `updated_at` | `.../realtime/maintenance_events/` |

### Invoices (PDF metadata)
| Entity | Natural Key | Silver Path |
|---|---|---|
| invoices | `invoice_id` | `.../invoices/` |
| quarantine | — | `.../quarantine/invoices/` |

### Reports (JSON)
| Report | MERGE Key | Silver Path |
|---|---|---|
| kpi_report | `report_period` | `.../reports/kpi_report/` |
| sla_report | `report_period` | `.../reports/sla_report/` |
| state_breakdown | `report_period` + `state_code` | `.../reports/state_breakdown/` |
| quarantine (each) | — | `.../quarantine/reports/<type>/` |

---

## Common errors

| Error | Cause | Fix |
|---|---|---|
| `Parameter 'load_type' was not provided` | Notebook run directly without job params | Run via Databricks Job only (v3 is production) |
| `No Bronze CSV files found for given partition` | Bronze task hasn't run yet or partition path wrong | Check: `dbutils.fs.ls(".../bronze-volume/realtime/charging_sessions/YYYY/MM/DD/HH/")` |
| `Bronze metadata table not found` | Invoice Bronze not run yet | Run `day_6/04_bronze_blob_invoices_pdf.ipynb` first |
| `<N> report file(s) missing in Bronze` | Report Bronze not run for that month | Run `day_6/05_bronze_blob_reports_json.ipynb` for same year/month |
| `AnalysisException: Path does not exist` | Silver Volume not created | Create `silver-volume` Volume under `dbw_ev_intelligence_dev.default` |
| `silver=0` for numeric entities | Corrupt check firing on legitimate NULLs | v3 uses pre-cast sentinel fix — this should not happen |
| Silver task shows as Skipped | Bronze task failed | Fix Bronze task failure first — Silver depends on Bronze success |
