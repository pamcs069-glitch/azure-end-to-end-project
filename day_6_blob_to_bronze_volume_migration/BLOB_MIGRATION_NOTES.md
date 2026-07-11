# Blob Migration Notes — All Realtime Entities → Bronze Volume
**Day 6 | Source Blob → Bronze Layer | All 5 Entities**

---

## What These Notebooks Do

Read raw CSV files from the instructor-provided source blob storage (`dataenggdailystorage`) and copy them into the Bronze Unity Catalog Volume for **all 5 realtime entities simultaneously**, preserving the exact source directory structure.

No transformation happens here. Files land in Bronze exactly as they are in the source. The Silver layer notebook handles schema enforcement, type casting, deduplication, and Delta write.

Same pattern as Day 3 `notebooks_source_blob_migration/` — extended from charging_sessions only to all entities.

---

## Entities Covered

| Entity | Source Subfolder | File Pattern |
|---|---|---|
| `charging_sessions` | `realtime/charging_sessions/` | `sessions_YYYYMMDD_HHMM.csv` |
| `charging_sessions_iot` | `realtime/charging_sessions_iot/` | `iot_YYYYMMDD_HHMM.csv` |
| `maintenance_events` | `realtime/maintenance_events/` | `maintenance_YYYYMMDD_HHMM.csv` |
| `energy_prices` | `realtime/energy_prices/` | `energy_YYYYMMDD_HHMM.csv` |
| `weather` | `realtime/weather/` | `weather_YYYYMMDD_HHMM.csv` |

---

## Directory Structure

### Source (instructor's blob storage)

```
wasbs://source@dataenggdailystorage.blob.core.windows.net/
  └── realtime/
        ├── charging_sessions/      YYYY/MM/DD/HH/  sessions_YYYYMMDD_HHMM.csv
        ├── charging_sessions_iot/  YYYY/MM/DD/HH/  iot_YYYYMMDD_HHMM.csv
        ├── maintenance_events/     YYYY/MM/DD/HH/  maintenance_YYYYMMDD_HHMM.csv
        ├── energy_prices/          YYYY/MM/DD/HH/  energy_YYYYMMDD_HHMM.csv
        └── weather/                YYYY/MM/DD/HH/  weather_YYYYMMDD_HHMM.csv
```

File naming pattern: one CSV per hour, one hour per folder.

### Bronze Volume (Unity Catalog — mirrors source exactly)

```
/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/
  └── realtime/
        ├── charging_sessions/      YYYY/MM/DD/HH/  *.csv
        ├── charging_sessions_iot/  YYYY/MM/DD/HH/  *.csv
        ├── maintenance_events/     YYYY/MM/DD/HH/  *.csv
        ├── energy_prices/          YYYY/MM/DD/HH/  *.csv
        └── weather/                YYYY/MM/DD/HH/  *.csv
```

The relative path under `realtime/<entity>/` is identical on both sides. No renaming, no flattening.

---

## Two Notebooks

| Notebook | Purpose | When to run |
|---|---|---|
| `01_bronze_blob_all_entities.ipynb` | **Manual** — you set the partition or run full load | First-time full load, ad-hoc backfills, debugging |
| `02_bronze_blob_all_entities_v2.ipynb` | **Scheduled** — Databricks Job every hour, auto 3-hour look-back | Normal daily operation |

---

## Notebook 1 (v1 — Manual)

### Cell reference

| Cell | What it does |
|---|---|
| Cell 1 | Authenticate to source blob via Key Vault secrets, set `SOURCE_ROOT` |
| Cell 2 | Set `LOAD_MODE`, partition variables, entity list — build paths per entity |
| Cell 3 | List source files per entity — confirm before copying |
| Cell 4 | Copy files from source to Bronze per entity |
| Cell 5 | Assert Bronze file count matches source per entity |
| Cell 6 | Read sample CSV per entity from Bronze, print schema |
| Cell 7 | Print run summary |

### Full load

```python
LOAD_MODE = "full"
```

Copies everything under `realtime/<entity>/` for all entities. Use for first run or full backfill.

### Incremental load

```python
LOAD_MODE = "incremental"

LOAD_YEAR  = "2026"
LOAD_MONTH = "07"
LOAD_DAY   = "11"
LOAD_HOUR  = "06"
```

Copies only the `YYYY/MM/DD/HH` partition across all entities simultaneously.

> Always zero-pad: `"06"` not `"6"`.

---

## Notebook 2 (v2 — Scheduled)

### What changed from v1

| | v1 | v2 |
|---|---|---|
| **Trigger** | Manual | Databricks Job — cron `0 * * * *` |
| **Partition** | Set manually per run | Auto-computed from `datetime.now(UTC)` |
| **Full load** | `LOAD_MODE = "full"` | `FULL_LOAD_OVERRIDE = True` (one-off flag) |
| **Look-back** | Single hour/full | 3-hour window — catches late-arriving data |
| **Already loaded** | Overwrites | Checks Bronze first — skips hours already present |
| **Source missing** | Crashes | Checks source first — exits cleanly if folder absent |

### How the 3-hour look-back works

The Job fires at the top of every hour (`0 * * * *`). At that point the previous hour has fully completed.

```
Job fires at 09:00 UTC → look-back window = [08, 07, 06] × 5 entities = 15 slots

For each entity-hour slot:
  ┌─ Check 1: Is it already in Bronze?
  │     YES → SKIP (already loaded on a previous run)
  │     NO  → continue
  └─ Check 2: Does the source folder exist?
        NO  → SKIP (data not arrived yet — retry on next run's window)
        YES → COPY to Bronze

Example — charging_sessions, 09:00 UTC run:
  charging_sessions 2026/07/11/08 → source exists, Bronze empty  → COPY
  charging_sessions 2026/07/11/07 → Bronze already has data      → SKIP
  charging_sessions 2026/07/11/06 → source not found yet          → SKIP
```

**Late-arriving data scenario:**
```
Run at 08:00 → checks [07, 06, 05] for all entities
  energy_prices 07 — source not found (late) → SKIP

Run at 09:00 → checks [08, 07, 06] for all entities
  energy_prices 07 — source now available    → COPY  ← automatically caught up
```

### Cell reference (v2)

| Cell | What it does | Needed for scheduled Job? |
|---|---|---|
| Cell 1 | Authenticate to source blob via Key Vault secrets | Yes — always |
| Cell 2 | Set entity list, build 3-hour look-back window | Yes — always |
| Cell 3 | Filter: skip already-in-Bronze OR source-missing slots | Yes — always |
| Cell 4 | List source files for each queued entity-hour | Yes — always |
| Cell 5 | Copy files to Bronze (auto-creates YYYY/MM/DD/HH/ folders) | Yes — main operation |
| Cell 6 | Assert Bronze count = source count per slot | Yes — failure triggers Job alert |
| Cell 7 | Read sample CSV from first copied slot, print schema | Optional |
| Cell 8 | Print run summary — visible in Job run history output | Yes |

---

## Prerequisites

Same as Day 3 — these must exist before running either notebook:

| Requirement | Where to set it up |
|---|---|
| `kv-ev-scope` Databricks secret scope | Day 1 Part 6.5 |
| `source-storage-account` in Key Vault | Day 1 — blob storage account name (`dataenggdailystorage`) |
| `source-container` in Key Vault | Day 1 — container name (`source`) |
| `source-sas-token` in Key Vault | Day 1 — SAS token with `sp=rl` (read + list) |
| Bronze Volume exists | Day 2 — `05_UNITY_CATALOG_EXTERNAL_LOCATIONS.md` Part 5 |
| Cluster attached to Unity Catalog metastore | Databricks workspace setup |

---

## Part A — Upload the Notebook to Databricks

1. Open your Databricks workspace
2. Left sidebar → **Workspace** → **Shared**
3. Click **⋮** → **Create** → **Folder** → name it `bronze_ingestion` (or open existing folder)
4. Inside the folder → click **⋮** → **Import**
5. Upload **both** notebooks:
   - `01_bronze_blob_all_entities.ipynb`
   - `02_bronze_blob_all_entities_v2.ipynb`

Confirm both appear at:
```
/Shared/bronze_ingestion/01_bronze_blob_all_entities
/Shared/bronze_ingestion/02_bronze_blob_all_entities_v2
```

---

## Part B — Run the Full Load First (One-Time, using v1)

Before scheduling the hourly Job, copy all existing historical data into Bronze.

1. Open `/Shared/bronze_ingestion/01_bronze_blob_all_entities`
2. In **Cell 2**, set `LOAD_MODE = "full"`
3. Click **Run all**
4. Wait for all cells to complete — this may take several minutes
5. Cell 5 will assert Bronze count matches source count per entity
6. Cell 7 prints final summary — confirm `Files failed: 0` for all entities

---

## Part C — Create the Databricks Job (using v2)

### Step 1 — Open Workflows

1. Left sidebar → **Workflows**
2. Click **+ Create job**

### Step 2 — Name the Job

3. Rename it to:
   ```
   job_bronze_all_entities_hourly
   ```

### Step 3 — Configure Task

4. **Task name:** `task_copy_all_entities_hourly`
5. **Type:** `Notebook`
6. **Source:** `Workspace`
7. **Path:** browse to `/Shared/bronze_ingestion/02_bronze_blob_all_entities_v2`
8. **Cluster:** select your existing `dev-cluster` (All-Purpose — already warm, no cold start)

### Step 4 — Set the Schedule

9. Click **Schedules & Triggers** tab → **+ Add schedule**
10. **Trigger type:** `Scheduled`
11. **Schedule:** `Custom cron`
12. **Cron expression:** `0 * * * *`
13. **Timezone:** `UTC`
14. Click **Save**

### Step 5 — Email Alerts

15. **Notifications** tab → **On failure** → **+ Add notification** → enter your email
16. Optionally add On success for the first few runs
17. Click **Save**

### Step 6 — Save and Activate

18. Click **Save job**
19. Toggle status from **Paused** → **Active**

### Step 7 — Verify Schedule

20. Confirm **Next run time** is shown (e.g. `2026-07-11 10:00:00 UTC`)

---

## Part D — Trigger a Manual Test Run

Before waiting for the next scheduled hour, trigger a run manually.

1. Databricks → **Workflows** → `job_bronze_all_entities_hourly`
2. Click **Run now**
3. Click into the run under **Active runs**
4. Watch cell-by-cell output — all cells should pass
5. Confirm Cell 8 summary shows `Files failed: 0` for all entities

> If all entity-hour slots are already loaded or source is missing, Cell 3 exits with:
> `INFO: Nothing to copy — all entity-hours already loaded or source data not yet available.`
> This is expected — the run is marked Succeeded.

---

## Part E — Monitor Scheduled Runs

1. Databricks → **Workflows** → `job_bronze_all_entities_hourly`
2. Click **Run history** tab
3. Each row = one run — click into any row for cell-by-cell output

### Healthy run (all 5 entities × 3 hours, most already loaded)

```
Cell 2: Job fire time (UTC): 2026-07-11 09:00:04 UTC
        Look-back window (15 slots — 3 hours × 5 entities):
          charging_sessions — 2026/07/11/08
          charging_sessions_iot — 2026/07/11/08
          ... (15 total)

Cell 3:   SKIP (already in Bronze) : charging_sessions — 2026/07/11/07
          SKIP (already in Bronze) : charging_sessions — 2026/07/11/06
          QUEUE for copy           : charging_sessions — 2026/07/11/08
          QUEUE for copy           : energy_prices — 2026/07/11/08
          ...
        Slots queued for copy: 5   (one per entity for the newest hour)

Cell 8: Total slots: 15 | Slots copied: 5 | Slots skipped: 10
          [COPIED]  charging_sessions — 2026/07/11/08
          [SKIPPED] charging_sessions — 2026/07/11/07   ← already in Bronze
          [COPIED]  energy_prices — 2026/07/11/08
          ...
```

### Nothing-to-do run (all already loaded)

```
Cell 3:   SKIP (already in Bronze) : charging_sessions — 2026/07/11/08
          ...
        Slots queued for copy: 0
        INFO: Nothing to copy — all entity-hours already loaded or source data not yet available.
Run status: Succeeded
```

---

## Part F — Verify Bronze Volume Contents

From any Databricks notebook:

```python
# List all hours loaded today for all entities
for entity in ["charging_sessions", "charging_sessions_iot",
               "maintenance_events", "energy_prices", "weather"]:
    try:
        files = dbutils.fs.ls(
            f"/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/{entity}/2026/07/11/"
        )
        print(f"{entity}: {len(files)} hour(s)")
    except Exception:
        print(f"{entity}: no data yet")

# Read a sample CSV from one entity
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/07/11/08/")

display(df.limit(10))
```

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `Secret does not exist: source-storage-account` | Secret name wrong or scope not set up | Add `source-storage-account`, `source-container`, `source-sas-token` to Key Vault |
| `Secret scope not found: kv-ev-scope` | Scope missing | Create scope in Databricks Settings → Secrets |
| `403 Forbidden` on `dbutils.fs.ls` (source) | SAS token expired or wrong permissions | Regenerate SAS with `sp=rl` (read + list), update Key Vault secret |
| `Path does not exist` (source) | Entity folder not present in that hour | Normal — Cell 3 skips it cleanly. Source data not yet arrived |
| `No such file or directory` (Bronze) | Volume not created in Unity Catalog | Day 2 `05_UNITY_CATALOG_EXTERNAL_LOCATIONS.md` — create Volume |
| Assertion fails in Cell 6 | Some files failed to copy | Check Cell 5 for `FAILED` lines, fix permission, re-run |
| Wrong hour loaded | Cluster not using UTC | Code uses `datetime.now(timezone.utc)` explicitly — should be correct |
| Job not firing | Job status is Paused | Workflows → job → toggle to **Active** |
| One entity copies but another skips | Entity folder missing in source blob | Check that source blob has all 5 entity subfolders under `realtime/` |

---

## Bronze Volume Path Reference

| Level | Path |
|---|---|
| Volume root | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/` |
| All realtime entities | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/` |
| One entity | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/` |
| One year | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/` |
| One month | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/07/` |
| One day | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/07/11/` |
| One hour | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/07/11/08/` |

Same structure applies to all 5 entities — just swap `charging_sessions` for the entity name.

---

## Cron Expression Reference

| Expression | Meaning |
|---|---|
| `0 * * * *` | Top of every hour (00:00, 01:00, ... 23:00 UTC) |
| `0 */2 * * *` | Every 2 hours |
| `0 9 * * *` | Once daily at 09:00 UTC |

Use `0 * * * *` — source writes one file per hour per entity.

---

## What Comes Next

Hourly raw CSVs now land in Bronze automatically for all realtime entities. The Silver layer notebook (Day 7) will:

1. Read CSVs from `/Volumes/.../bronze-volume/realtime/<entity>/`
2. Apply explicit schema (cast string columns to correct types)
3. Deduplicate by entity primary key
4. Write as Delta table to the Silver Volume

No changes needed to these notebooks before that — Silver reads from the same Bronze Volume paths.
