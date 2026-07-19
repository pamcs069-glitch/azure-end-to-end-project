# Job 2 — `job_bronze_invoices_daily`
**Notebook:** `04_bronze_blob_invoices_pdf.ipynb`
**Schedule:** Daily at 01:00 UTC — cron `0 1 * * *`
**Parameter:** `load_type = incremental`

---

## What This Job Does

Fires at 01:00 UTC every day. Targets yesterday's date partition (source invoices are finalized by end of business day) and copies all ~15 PDF invoices into Bronze.

```
Job fires 2026-07-11 01:00 UTC
  → incremental target: 2026/07/10  (yesterday)
  → source: wasbs://.../invoices/2026/07/10/*.pdf
  → bronze: /Volumes/.../bronze-volume/invoices/2026/07/10/*.pdf
```

**Incremental partition:** `YYYY/MM/DD/` — yesterday, auto-computed from `datetime.now(UTC) - 1 day`
**Full load partition:** entire `invoices/` tree — all months, all days

---

## Step-by-Step: Create the Job

### Step 1 — Open Workflows

1. Databricks left sidebar → **Workflows**
2. Click **+ Create job**

---

### Step 2 — Name the Job

3. Click the default title → rename to:
   ```
   job_bronze_invoices_daily
   ```

---

### Step 3 — Configure the Task

4. **Task name:** `task_invoices_copy`
5. **Type:** `Notebook`
6. **Source:** `Workspace`
7. **Path:** click the folder icon → navigate to:
   ```
   /Shared/bronze_ingestion/04_bronze_blob_invoices_pdf
   ```
8. **Cluster:** select `dev-cluster` (All-Purpose)

---

### Step 4 — Add the `load_type` Parameter

9. Scroll down to **Parameters** → click **+ Add**
10. Fill in:
    - **Key:** `load_type`
    - **Value:** `incremental`

> Notebook Cell 2 reads this with `dbutils.widgets.get("load_type")` and computes yesterday's date automatically. You never need to hardcode a date.

---

### Step 5 — Set the Schedule

11. Click **Schedules & Triggers** tab → **+ Add schedule**
12. Fill in:
    - **Trigger type:** `Scheduled`
    - **Schedule:** `Custom cron`
    - **Cron expression:** `0 1 * * *`
    - **Timezone:** `UTC`

    > `0 1 * * *` = 01:00 UTC every day.
    > Source invoices are written the previous day — 01:00 UTC ensures the day is fully closed before we copy.

13. Click **Save**

---

### Step 6 — Email Alerts

14. **Notifications** tab → **On failure** → **+ Add notification** → enter your email
15. Click **Save**

---

### Step 7 — Save and Activate

16. Click **Save job**
17. Toggle from **Paused** → **Active**
18. Confirm **Next run time** shown (e.g. `2026-07-12 01:00:00 UTC`)

---

## Step 8 — First-Time Full Load

Seed Bronze with all historical PDF invoices before the daily job takes over.

1. **Workflows** → `job_bronze_invoices_daily`
2. Click **Run now with different parameters**
3. Change `load_type` to `full`
4. Click **Run**
5. Click into the run → wait for Cell 5 verification
6. Cell 7 summary: confirm `PDFs copied ≈ 450`, `PDFs failed = 0`

> The full load copies all ~450 PDFs (June 2026, 30 days × 15/day). Takes a few minutes.

---

## Step 9 — Test with Manual Run (incremental)

1. **Workflows** → `job_bronze_invoices_daily` → **Run now** (uses saved `incremental` param)
2. Cell 2 output will show:
   ```
   Mode : incremental — 2026/07/10  (yesterday UTC)
   Source : wasbs://.../invoices/2026/07/10/
   ```
3. Cell 5 verifies file count matches
4. Cell 7 summary shows counts

---

## Healthy Run Output Reference

### Normal daily incremental

```
Cell 2:
  load_type : incremental
  Mode : incremental — 2026/07/10  (yesterday UTC)
  Source : wasbs://source@dataenggdailystorage.../invoices/2026/07/10/
  Bronze : /Volumes/.../bronze-volume/invoices/2026/07/10/

Cell 4:
  COPIED  INV-AU-2026-0002701.pdf
  COPIED  INV-AU-2026-0002702.pdf
  ... (15 files)
  Result: 15 copied, 0 failed

Cell 7:
  load_type : incremental
  Date (UTC-1d) : 2026/07/10
  PDFs copied : 15
  PDFs failed : 0
```

### Source folder not yet available (data late)

```
Cell 3:
  PDF files found: 0
  Total size: 0.0 MB

Cell 7:
  PDFs copied: 0
Run status: Succeeded  ← exits cleanly, no error raised
```

> If source invoices are delayed, the job exits without error. Re-run manually once data arrives, or wait — next day's run will not retry yesterday automatically. Run with `full` to backfill.

---

## How to Backfill a Missed Day

If a daily run is missed or source data was late:

1. **Workflows** → `job_bronze_invoices_daily` → **Run now with different parameters**
2. Set `load_type = full` (this copies all missing dates)
3. Or: open `04_bronze_blob_invoices_pdf.ipynb` directly, set `LOAD_MODE = "daily"` and hardcode the date in Cell 2, run manually

---

## Cron Reference

| Expression | Meaning |
|---|---|
| `0 1 * * *` | 01:00 UTC every day ← **this job** |
| `0 0 * * *` | Midnight UTC every day |
| `0 6 * * *` | 06:00 UTC every day |

---

## Verify Bronze Contents

```python
# Run in any Databricks notebook
import datetime
yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y/%m/%d")
files = dbutils.fs.ls(
    f"/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/invoices/{yesterday}/"
)
print(f"Yesterday ({yesterday}): {len(files)} PDF(s) in Bronze")
```
