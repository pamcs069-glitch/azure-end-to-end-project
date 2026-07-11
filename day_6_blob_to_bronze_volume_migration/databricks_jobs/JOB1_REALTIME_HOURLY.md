# Job 1 — `job_bronze_realtime_hourly`
**Notebook:** `02_bronze_blob_all_entities_v2.ipynb`
**Schedule:** Every hour — cron `0 * * * *`
**Parameter:** `load_type = incremental`

---

## What This Job Does

Fires at the top of every hour. Checks the 3 most recently completed hours for each realtime entity (`charging_sessions`, `maintenance_events`) and copies any CSV files not yet in Bronze.

```
Job fires 09:00 UTC → checks hours 08, 07, 06 × 2 entities = 6 slots

For each slot:
  Already in Bronze? → SKIP
  Source folder missing? → SKIP (late data — retried on next run's window)
  Otherwise → COPY
```

**Incremental partition:** `YYYY/MM/DD/HH/` — auto-computed from `datetime.now(UTC) - N hours`
**Full load partition:** entire `realtime/<entity>/` tree

---

## Step-by-Step: Create the Job

### Step 1 — Open Workflows

1. Databricks left sidebar → **Workflows**
2. Click **+ Create job**

---

### Step 2 — Name the Job

3. Click the default title (`New job`) at the top → rename to:
   ```
   job_bronze_realtime_hourly
   ```

---

### Step 3 — Configure the Task

You are on the **Tasks** tab with one pre-created task.

4. **Task name:** `task_realtime_copy`
5. **Type:** `Notebook`
6. **Source:** `Workspace`
7. **Path:** click the folder icon → navigate to:
   ```
   /Shared/bronze_ingestion/02_bronze_blob_all_entities_v2
   ```
8. **Cluster:** select `dev-cluster` (All-Purpose — already warm, no cold start delay)

---

### Step 4 — Add the `load_type` Parameter

Still on the Tasks tab, scroll down to **Parameters**.

9. Click **+ Add**
10. Fill in:
    - **Key:** `load_type`
    - **Value:** `incremental`

> This passes `load_type=incremental` as a Databricks widget to the notebook at runtime.
> The notebook reads it with `dbutils.widgets.get("load_type")` in Cell 2.

---

### Step 5 — Set the Schedule

11. Click the **Schedules & Triggers** tab
12. Click **+ Add schedule**
13. Fill in:
    - **Trigger type:** `Scheduled`
    - **Schedule:** `Custom cron`
    - **Cron expression:** `0 * * * *`
    - **Timezone:** `UTC`

    > `0 * * * *` = minute 0 of every hour = 00:00, 01:00, ... 23:00 UTC every day

14. Click **Save**

---

### Step 6 — Email Alerts

15. Click **Notifications** tab
16. **On failure** → **+ Add notification** → enter your email
17. Optionally add On success for the first few runs
18. Click **Save**

---

### Step 7 — Save and Activate

19. Click **Save job** (top right)
20. Toggle from **Paused** → **Active**
21. Confirm **Next run time** is shown (e.g. `2026-07-11 10:00:00 UTC`)

---

## Step 8 — First-Time Full Load

Before the scheduled incremental runs start, seed Bronze with all historical data.

1. Databricks → **Workflows** → `job_bronze_realtime_hourly`
2. Click **Run now with different parameters** (dropdown next to Run now)
3. Change `load_type` value from `incremental` to `full`
4. Click **Run**
5. Click into the active run → watch Cell 8 summary
6. Confirm `Total files copied > 0` and `Total files failed = 0`

> After the full load completes, scheduled runs will use `incremental` automatically.
> You do not need to change anything — the Job parameter stays as `incremental`.

---

## Step 9 — Test the Scheduled Run

Trigger a manual test with the normal `incremental` parameter:

1. **Workflows** → `job_bronze_realtime_hourly`
2. Click **Run now** (uses the saved `incremental` parameter)
3. Click into the run → watch Cell 3 output:
   - `SKIP (already in Bronze)` = normal if previous hours already loaded
   - `SKIP (source not found)` = normal if current hour not yet written by source
   - `QUEUE for copy` + `COPIED` = new data found and loaded
4. Cell 8 summary prints counts — `Files failed: 0` means healthy run

---

## Healthy Run Output Reference

### First incremental after full load (all slots already loaded)

```
Cell 3:
  SKIP (already in Bronze) : charging_sessions — 2026/07/11/08
  SKIP (already in Bronze) : maintenance_events — 2026/07/11/08
  ...
  Slots queued for copy: 0
  INFO: Nothing to copy — load_type=incremental, all slots already loaded or source not found.
Run status: Succeeded
```

### Normal steady-state (new hour just landed in source)

```
Cell 3:
  QUEUE for copy : charging_sessions — 2026/07/11/09
  QUEUE for copy : maintenance_events — 2026/07/11/09
  SKIP (already in Bronze) : charging_sessions — 2026/07/11/08
  ...
  Slots queued for copy: 2

Cell 8:
  load_type: incremental
  Slots copied: 2 | Slots skipped: 4 | Files copied: 2
```

---

## Cron Reference

| Expression | Meaning |
|---|---|
| `0 * * * *` | Top of every hour ← **this job** |
| `0 */2 * * *` | Every 2 hours |
| `0 6 * * *` | Once daily at 06:00 UTC |

---

## How to Run a One-Off Full Load Later

If Bronze data is lost or you need to backfill:

1. **Workflows** → `job_bronze_realtime_hourly` → **Run now with different parameters**
2. Set `load_type = full`
3. Run — copies all source history into Bronze (overwrites existing files)
4. Scheduled incremental runs continue unchanged after

---

## Verify Bronze Contents

```python
# Run in any Databricks notebook
for entity in ["charging_sessions", "maintenance_events"]:
    files = dbutils.fs.ls(
        f"/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/{entity}/"
    )
    print(f"{entity}: {len(files)} year folders")
```
