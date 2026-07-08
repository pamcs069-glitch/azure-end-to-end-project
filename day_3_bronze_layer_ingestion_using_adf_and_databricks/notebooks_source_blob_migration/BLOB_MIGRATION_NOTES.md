# Blob Migration Notes — Charging Sessions → Bronze Volume
**Day 3 | Source Blob → Bronze Layer**

---

## What This Notebook Does

Reads raw CSV files from the instructor-provided source blob storage (`dataenggdailystorage`) and copies them into the Bronze Unity Catalog Volume, preserving the exact source directory structure.

No transformation happens here. Files land in Bronze exactly as they are in the source. The Silver layer notebook handles schema enforcement, type casting, deduplication, and Delta write.

---

## Directory Structure

### Source (instructor's blob storage)

```
wasbs://source@dataenggdailystorage.blob.core.windows.net/
  └── realtime/
        └── charging_sessions/
              └── 2026/
                    └── 06/
                          └── 01/
                                └── 06/
                                      └── sessions_20260601_0600.csv
```

File naming pattern: `sessions_YYYYMMDD_HHMM.csv`
One CSV per hour, one hour per folder.

### Bronze Volume (Unity Catalog — mirrors source exactly)

```
/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/
  └── realtime/
        └── charging_sessions/
              └── 2026/
                    └── 06/
                          └── 01/
                                └── 06/
                                      └── sessions_20260601_0600.csv
```

The relative path under `realtime/charging_sessions/` is identical on both sides. No renaming, no flattening.

---

## Full Load vs Incremental Load

Only **Cell 2** changes between runs. Everything else stays the same.

### Full Load

```python
LOAD_MODE = "full"
```

Copies everything under `realtime/charging_sessions/` — all years, all months, all days, all hours.

Use for:
- First run (nothing in Bronze yet)
- Re-seeding Bronze after a data issue
- Backfilling historical data

### Incremental Load

```python
LOAD_MODE = "incremental"

LOAD_YEAR  = "2026"
LOAD_MONTH = "06"
LOAD_DAY   = "01"
LOAD_HOUR  = "06"
```

Copies only the single hour folder: `realtime/charging_sessions/2026/06/01/06/`

Use for:
- Daily or hourly scheduled runs
- Copying only the current session's new files

> The year/month/day/hour values must match the folder names in the source blob **exactly** — always zero-padded (use `"06"` not `"6"`).

---

## How the Path Is Built

```
Full mode:
  source_path = wasbs://source@dataenggdailystorage.blob.core.windows.net/realtime/charging_sessions/
  bronze_path = /Volumes/.../bronze-volume/realtime/charging_sessions/

Incremental mode:
  source_path = wasbs://source@dataenggdailystorage.blob.core.windows.net/realtime/charging_sessions/2026/06/01/06/
  bronze_path = /Volumes/.../bronze-volume/realtime/charging_sessions/2026/06/01/06/
```

The relative path extracted from each source file is appended directly to `bronze_path` — this is what keeps folder structure identical on both sides.

---

## Prerequisites

| Requirement | Where to set it up |
|---|---|
| `kv-ev-scope` Databricks secret scope | Day 1 Part 6.5 |
| `source-sas-token` in Key Vault | Day 1 — SAS token for `dataenggdailystorage` container `source` |
| Unity Catalog Bronze Volume exists | Day 2 — `05_UNITY_CATALOG_EXTERNAL_LOCATIONS.md` Part 5 |
| Cluster attached to Unity Catalog metastore | Databricks workspace setup |

---

## Secret Scope Setup (if not done)

The SAS token grants read + list access to the `source` container. Store it in Key Vault and expose it via the Databricks secret scope.

**Key Vault secret name:** `source-sas-token`
**Secret value format:** `se=2027-07-30&sp=rl&spr=https&sv=2026-04-06&sr=c&sig=...`

In Databricks — create scope linked to Key Vault (if not already done):
```
Databricks UI → Settings → Secrets → Create Secret Scope
  Name: kv-ev-scope
  Manage Principal: Creator
  DNS Name: https://kv-ev-intelligence-dev.vault.azure.net/
  Resource ID: /subscriptions/<sub-id>/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.KeyVault/vaults/kv-ev-intelligence-dev
```

---

## Cell-by-Cell Reference

| Cell | What it does | Must re-run after cluster restart? |
|---|---|---|
| Cell 1 | Authenticates to source blob via SAS token, sets `SOURCE_ROOT` | Yes — Spark config clears on restart |
| Cell 2 | Sets `LOAD_MODE`, partition variables, builds `source_path` and `bronze_path` | Yes — variables clear on restart |
| Cell 3 | Lists source files — confirms what will be copied | Optional — useful for verification |
| Cell 4 | Copies files from source to Bronze Volume | Yes — this is the main operation |
| Cell 5 | Lists files in Bronze Volume — confirms copy was successful | Optional |
| Cell 6 | Reads one CSV from Bronze into Spark, prints schema and 5 rows | Optional — schema inspection only |
| Cell 7 | Prints run summary | Optional |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `Secret does not exist: source-sas-token` | Secret not in Key Vault or wrong name | Add `source-sas-token` to KV with exact name |
| `Secret scope not found: kv-ev-scope` | Scope not created or cluster restarted | Re-create scope or re-run Cell 1 after attach |
| `403 Forbidden` on `dbutils.fs.ls` | SAS token expired or wrong permissions | Regenerate SAS with `sp=rl` (read + list), update KV secret |
| `Path does not exist` on source `ls` | Wrong year/month/day/hour values in Cell 2 | Check portal — folder names are zero-padded (`06` not `6`) |
| `No such file or directory` on Bronze Volume | Volume not created in Unity Catalog | Day 2 `05_UNITY_CATALOG_EXTERNAL_LOCATIONS.md` — create Volume first |
| Copy completes but Bronze file count < source count | Some files failed in Cell 4 | Check `skipped` list printed at end of Cell 4, investigate errors |
| `inferSchema` reads all columns as string | CSV has inconsistent types or nulls | Expected at Bronze — Silver layer will cast types explicitly |

---

## Bronze Volume Path Reference

| Level | Path |
|---|---|
| Volume root | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/` |
| All charging sessions | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/` |
| One year | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/` |
| One month | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/06/` |
| One day | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/06/01/` |
| One hour | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/06/01/06/` |
| One file | `/Volumes/dbw_ev_intelligence_dev/default/bronze-volume/realtime/charging_sessions/2026/06/01/06/sessions_20260601_0600.csv` |

---

## What Comes Next

This notebook lands raw CSVs in Bronze. The next notebook (Silver layer — Day 7) will:

1. Read all CSVs from `/Volumes/.../bronze-volume/realtime/charging_sessions/`
2. Apply explicit schema (cast string columns to correct types)
3. Deduplicate by `session_id`
4. Write as Delta table to the Silver Volume

No code in this notebook needs to change before that — Silver reads from the same Bronze Volume path.
