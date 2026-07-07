# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer v2 — Hourly Scheduled Load: Source Blob → Charging Sessions
# MAGIC
# MAGIC **Scheduled via Databricks Job — runs every hour automatically.**
# MAGIC
# MAGIC Reads the current hour's CSV files from the instructor source blob and copies them
# MAGIC into the Bronze Volume, preserving the exact source directory structure.
# MAGIC
# MAGIC **Source layout:**
# MAGIC ```
# MAGIC wasbs://source@dataenggdailystorage.blob.core.windows.net/
# MAGIC   └── realtime/charging_sessions/YYYY/MM/DD/HH/
# MAGIC         └── sessions_YYYYMMDD_HHMM.csv
# MAGIC ```
# MAGIC
# MAGIC **Bronze target (mirrors source exactly):**
# MAGIC ```
# MAGIC /Volumes/dbw_ev_intelligence_dev/default/bronze-volume/
# MAGIC   └── realtime/charging_sessions/YYYY/MM/DD/HH/
# MAGIC         └── sessions_YYYYMMDD_HHMM.csv
# MAGIC ```
# MAGIC
# MAGIC **Difference from v1:**
# MAGIC | | v1 | v2 |
# MAGIC |---|---|---|
# MAGIC | How year/month/day/hour is set | Manually edited in Cell 2 | Auto-computed from system clock at runtime |
# MAGIC | Load modes | `full` or `incremental` (manual) | Always loads current hour (scheduled) |
# MAGIC | Full load | Supported via `LOAD_MODE = "full"` | Separate parameter `FULL_LOAD_OVERRIDE` for one-off full loads |
# MAGIC | Scheduling | Run manually | Databricks Job — every hour via cron `0 * * * *` |
# MAGIC | Missing hour handling | Crashes with `Path does not exist` | Logs warning and exits cleanly |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Authenticate to source blob
# MAGIC
# MAGIC Reads credentials from Key Vault via `kv-ev-scope` secret scope.
# MAGIC Must run every session — Spark config does not persist across cluster restarts.

# COMMAND ----------

from datetime import datetime, timezone

SCOPE = "kv-ev-scope"

STORAGE_ACCOUNT = dbutils.secrets.get(scope=SCOPE, key="source-storage-account")
CONTAINER       = dbutils.secrets.get(scope=SCOPE, key="source-container")
SAS_TOKEN       = dbutils.secrets.get(scope=SCOPE, key="source-sas-token")

spark.conf.set(
    f"fs.azure.sas.{CONTAINER}.{STORAGE_ACCOUNT}.blob.core.windows.net",
    SAS_TOKEN
)

SOURCE_ROOT = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"

print(f"Storage account : {STORAGE_ACCOUNT}")
print(f"Container       : {CONTAINER}")
print(f"SAS token       : [REDACTED]")
print(f"Source root     : {SOURCE_ROOT}")
print("Source blob authenticated — OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Resolve load partition from system clock
# MAGIC
# MAGIC **This cell never needs manual editing when running as a scheduled Job.**
# MAGIC
# MAGIC At runtime, `datetime.now(UTC)` gives the current wall-clock time.
# MAGIC The Job fires at the top of every hour (e.g. 09:00 UTC) so `HH` always
# MAGIC matches the folder that the source system just finished writing.
# MAGIC
# MAGIC **`FULL_LOAD_OVERRIDE`** — set to `True` only when you need to re-copy
# MAGIC all historical data (first run, disaster recovery). Leave `False` for all
# MAGIC normal scheduled runs.
# MAGIC
# MAGIC Zero-padding is applied automatically — `6` becomes `"06"`.

# COMMAND ----------

# Set to True only for a one-off full historical load.
# Leave False for all normal hourly scheduled runs.
FULL_LOAD_OVERRIDE = False

now = datetime.now(timezone.utc)

LOAD_YEAR  = now.strftime("%Y")   # e.g. "2026"
LOAD_MONTH = now.strftime("%m")   # e.g. "07"
LOAD_DAY   = now.strftime("%d")   # e.g. "06"
LOAD_HOUR  = now.strftime("%H")   # e.g. "09"

BRONZE_VOLUME = "/Volumes/dbw_ev_intelligence_dev/default/bronze-volume"
BASE_SUBPATH  = "realtime/charging_sessions"

if FULL_LOAD_OVERRIDE:
    source_path = f"{SOURCE_ROOT}/{BASE_SUBPATH}/"
    bronze_path = f"{BRONZE_VOLUME}/{BASE_SUBPATH}/"
    load_label  = "FULL (override)"
else:
    partition   = f"{LOAD_YEAR}/{LOAD_MONTH}/{LOAD_DAY}/{LOAD_HOUR}"
    source_path = f"{SOURCE_ROOT}/{BASE_SUBPATH}/{partition}/"
    bronze_path = f"{BRONZE_VOLUME}/{BASE_SUBPATH}/{partition}/"
    load_label  = f"INCREMENTAL — {partition}"

print(f"Run time (UTC)  : {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"Load mode       : {load_label}")
print(f"Source path     : {source_path}")
print(f"Bronze path     : {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Check source folder exists
# MAGIC
# MAGIC Before attempting to list or copy files, verify the source hour folder exists.
# MAGIC
# MAGIC **Why this matters for scheduling:** The source system may write the CSV a few
# MAGIC minutes after the hour boundary. If the Job fires at exactly 09:00 and the file
# MAGIC arrives at 09:03, the first run would crash without this check.
# MAGIC
# MAGIC If the folder is missing the notebook exits cleanly with a warning — the Job
# MAGIC marks the run as Succeeded (not Failed) so no alert fires for a normal late-arrival.
# MAGIC Adjust `dbutils.notebook.exit` to a non-zero code if you want the Job to alert on missing hours.

# COMMAND ----------

def folder_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

if not folder_exists(source_path):
    msg = f"Source folder not found — {source_path}. Data may not have arrived yet. Exiting."
    print(f"WARNING: {msg}")
    dbutils.notebook.exit(msg)

print(f"Source folder confirmed — {source_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — List source files
# MAGIC
# MAGIC Lists all files under the resolved source path.
# MAGIC For the scheduled hourly job this will typically be one CSV file per run.
# MAGIC For a full load override it recurses all year/month/day/hour subdirectories.

# COMMAND ----------

def list_files_recursive(path):
    items = dbutils.fs.ls(path)
    files = []
    for item in items:
        if item.isDir():
            files.extend(list_files_recursive(item.path))
        else:
            files.append(item)
    return files

source_files = list_files_recursive(source_path)

if not source_files:
    msg = f"No files found at source path — {source_path}. Exiting."
    print(f"WARNING: {msg}")
    dbutils.notebook.exit(msg)

print(f"Files found: {len(source_files)}")
for f in source_files:
    size_kb = round(f.size / 1024, 1)
    print(f"  {f.path}  [{size_kb} KB]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Copy files to Bronze Volume
# MAGIC
# MAGIC Copies each source file to the Bronze Volume, recreating the exact
# MAGIC directory structure by stripping the `source_path` prefix and appending
# MAGIC the relative path to `bronze_path`.
# MAGIC
# MAGIC Files already present at the destination are overwritten — this makes the
# MAGIC job idempotent: re-running the same hour always produces the same result.

# COMMAND ----------

copied  = []
skipped = []

for file_info in source_files:
    relative_path = file_info.path.replace(source_path, "")
    dest_path     = bronze_path + relative_path

    try:
        dbutils.fs.cp(file_info.path, dest_path)
        copied.append(dest_path)
        print(f"  COPIED  {relative_path}")
    except Exception as e:
        skipped.append((file_info.path, str(e)))
        print(f"  FAILED  {relative_path} — {e}")

print(f"\nCopy complete: {len(copied)} copied, {len(skipped)} failed")

if skipped:
    raise Exception(f"{len(skipped)} file(s) failed to copy — check output above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Verify files in Bronze Volume
# MAGIC
# MAGIC Lists files at the destination path and asserts the count matches the source.
# MAGIC If the assertion fails, the Job run is marked as Failed and an alert fires.

# COMMAND ----------

bronze_files = list_files_recursive(bronze_path)

print(f"Files in Bronze Volume: {len(bronze_files)}")
for f in bronze_files:
    size_kb = round(f.size / 1024, 1)
    print(f"  {f.path}  [{size_kb} KB]")

assert len(bronze_files) == len(source_files), (
    f"File count mismatch — source: {len(source_files)}, bronze: {len(bronze_files)}"
)
print("Verification passed — source and Bronze file counts match.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — Read sample file and confirm schema
# MAGIC
# MAGIC Reads the first copied CSV into Spark and prints schema + 5 rows.
# MAGIC This is a lightweight sanity check — not required for production runs
# MAGIC but useful when setting up the job for the first time.

# COMMAND ----------

sample_file = bronze_files[0].path
print(f"Reading sample: {sample_file}")

df = spark.read.option("header", True).option("inferSchema", True).csv(sample_file)

print(f"Row count : {df.count():,}")
print(f"Columns   : {len(df.columns)}")
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8 — Run summary
# MAGIC
# MAGIC Prints final summary. In a scheduled Job this appears in the Job run output
# MAGIC and is visible in Databricks → Workflows → Job runs → this run → Output.

# COMMAND ----------

print("=" * 60)
print("BRONZE BLOB MIGRATION v2 — HOURLY RUN SUMMARY")
print("=" * 60)
print(f"Run time (UTC)  : {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"Load mode       : {load_label}")
print(f"Source path     : {source_path}")
print(f"Bronze path     : {bronze_path}")
print(f"Files copied    : {len(copied)}")
print(f"Files failed    : {len(skipped)}")
print("=" * 60)
print("Next step: Silver layer reads from Bronze Volume and writes Delta.")
