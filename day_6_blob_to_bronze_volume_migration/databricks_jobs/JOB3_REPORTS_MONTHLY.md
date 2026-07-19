# Job 3 — `job_bronze_reports_monthly`
**Notebook:** `05_bronze_blob_reports_json.ipynb`
**Schedule:** 1st of every month at 02:00 UTC — cron `0 2 1 * *`
**Parameter:** `load_type = incremental`

---

## What This Job Does

Fires at 02:00 UTC on the 1st of every month. Targets last month's `YYYY/MM/` folder and copies all 3 monthly report JSONs (KPI, SLA, state breakdown) into Bronze.

```
Job fires 2026-08-01 02:00 UTC
  → incremental target: 2026/07  (last month)
  → source: wasbs://.../reports/2026/07/*.json
  → bronze: /Volumes/.../bronze-volume/reports/2026/07/*.json
  → 3 files: kpi_report_202607.json, sla_report_202607.json, state_breakdown_202607.json
```

**Incremental partition:** `YYYY/MM/` — last month, auto-computed from `datetime.now(UTC) - 1 month`
**Full load partition:** entire `reports/` tree — all months

---

## Step-by-Step: Create the Job

### Step 1 — Open Workflows

1. Databricks left sidebar → **Workflows**
2. Click **+ Create job**

---

### Step 2 — Name the Job

3. Click the default title → rename to:
   ```
   job_bronze_reports_monthly
   ```

---

### Step 3 — Configure the Task

4. **Task name:** `task_reports_copy`
5. **Type:** `Notebook`
6. **Source:** `Workspace`
7. **Path:** click the folder icon → navigate to:
   ```
   /Shared/bronze_ingestion/05_bronze_blob_reports_json
   ```
8. **Cluster:** select `dev-cluster` (All-Purpose)

---

### Step 4 — Add the `load_type` Parameter

9. Scroll down to **Parameters** → click **+ Add**
10. Fill in:
    - **Key:** `load_type`
    - **Value:** `incremental`

> Notebook Cell 2 reads this with `dbutils.widgets.get("load_type")` and uses `dateutil.relativedelta` to compute last month automatically. You never need to hardcode a year/month.

---

### Step 5 — Set the Schedule

11. Click **Schedules & Triggers** tab → **+ Add schedule**
12. Fill in:
    - **Trigger type:** `Scheduled`
    - **Schedule:** `Custom cron`
    - **Cron expression:** `0 2 1 * *`
    - **Timezone:** `UTC`

    > `0 2 1 * *` = 02:00 UTC on the 1st day of every month.
    > Monthly reports are generated after month-end close — the 1st of the next month ensures they are finalized.

13. Click **Save**

---

### Step 6 — Email Alerts

14. **Notifications** tab → **On failure** → **+ Add notification** → enter your email
15. Click **Save**

---

### Step 7 — Save and Activate

16. Click **Save job**
17. Toggle from **Paused** → **Active**
18. Confirm **Next run time** shown (e.g. `2026-08-01 02:00:00 UTC`)

---

## Step 8 — First-Time Full Load

Seed Bronze with all existing monthly report JSONs before the monthly job takes over.

1. **Workflows** → `job_bronze_reports_monthly`
2. Click **Run now with different parameters**
3. Change `load_type` to `full`
4. Click **Run**
5. Cell 5 verifies count, Cell 7 prints summary
6. Confirm `JSON files copied = 3` (June 2026), `JSON files failed = 0`

---

## Step 9 — Test with Manual Run (incremental)

> Note: If you run this in July 2026 with `load_type = incremental`, it targets June 2026 (`last month`). If June data is already in Bronze from the full load, the source files still get copied (incremental does not skip already-loaded months for reports — 3 files are tiny, re-copy is safe).

1. **Workflows** → `job_bronze_reports_monthly` → **Run now**
2. Cell 2 output:
   ```
   load_type : incremental
   Mode : incremental — 2026/06  (last month UTC)
   Source : wasbs://source@dataenggdailystorage.../reports/2026/06/
   ```
3. Cell 5 verifies 3 files
4. Cell 7 shows filenames

---

## Healthy Run Output Reference

### Normal monthly run

```
Cell 2:
  load_type : incremental
  Mode : incremental — 2026/07  (last month UTC)
  Source : wasbs://source@dataenggdailystorage.../reports/2026/07/
  Bronze : /Volumes/.../bronze-volume/reports/2026/07/

Cell 4:
  COPIED  kpi_report_202607.json
  COPIED  sla_report_202607.json
  COPIED  state_breakdown_202607.json
  Result: 3 copied, 0 failed

Cell 7:
  load_type         : incremental
  Month (UTC-1m)    : 2026/07
  JSON files copied : 3
  JSON files failed : 0
    kpi_report_202607.json
    sla_report_202607.json
    state_breakdown_202607.json
```

### Source month not yet available

```
Cell 3:
  JSON files found: 0

Cell 7:
  JSON files copied: 0
Run status: Succeeded  ← exits cleanly, no assertion raised on empty
```

---

## How to Load a Specific Month Manually

If you need to load a specific past month that was missed:

1. Open `05_bronze_blob_reports_json.ipynb` directly in Databricks
2. In Cell 2, the widget default is `incremental` — override it by typing `full` in the widget dropdown
3. Or: use **Run now with different parameters** → set `load_type = full` to copy all months

For a single specific month without full load:
1. Open the notebook
2. Run Cell 1 (auth)
3. Manually set variables and run from Cell 3:
```python
partition   = "2026/05"
SOURCE_PATH = f"{SOURCE_ROOT}/reports/{partition}/"
BRONZE_PATH = f"{BRONZE_VOLUME}/reports/{partition}/"
```

---

## Cron Reference

| Expression | Meaning |
|---|---|
| `0 2 1 * *` | 02:00 UTC on the 1st of every month ← **this job** |
| `0 0 1 * *` | Midnight UTC on the 1st of every month |
| `0 2 L * *` | 02:00 UTC on the last day of every month (not standard cron — use `0 2 28-31 * *` instead) |

---

## Verify Bronze Contents

```python
# Run in any Databricks notebook
files = dbutils.fs.ls(
    "/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/reports/"
)
for year_dir in files:
    months = dbutils.fs.ls(year_dir.path)
    for month_dir in months:
        reports = dbutils.fs.ls(month_dir.path)
        print(f"  {month_dir.path.split('reports/')[-1]} → {len(reports)} file(s)")
```
