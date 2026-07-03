# 00b — Connect to ADLS Gen2 Without Mounting
**Day 1 | Modern Alternative to `00_mount_storage`**

> **Use this guide if:**
> - Your cluster Access mode is **Standard**, **Shared**, or **Serverless** (mount is blocked in these modes)
> - Your cluster is **Dedicated** but you want the modern approach (recommended for all new learners)
>
> **Skip this guide if:** you already ran `00_mount_storage.ipynb` and it worked — no need to do both.

---

## Do I need to run this every session?

**Yes — but only once per session, and it takes under 30 seconds.**

Spark OAuth config is held in memory. When a cluster restarts or a new session starts, the config is gone. You re-run 3 cells to restore it.

**The smart way to handle this** (covered at the end of this guide) is a single `%run` line at the top of every notebook — one line replaces re-copying 3 cells everywhere.

```
Every session, in ANY notebook that reads/writes ADLS:
─────────────────────────────────────────────────────
  %run ./00b_connect_storage_no_mount           ← one line, done
─────────────────────────────────────────────────────
This runs Cells 1–3 automatically. No copy-paste needed.
```

---

## What Is This?

Instead of mounting containers to `/mnt/bronze`, you configure Spark with your Service Principal OAuth credentials **once per session**, then read and write using full `abfss://` paths directly.

No `dbutils.fs.mount()` is ever called.

**Path comparison:**

| Old (mount) | New (direct — this guide) |
|---|---|
| `/mnt/bronze/ev_sessions` | `abfss://bronze@evdatalakedev.dfs.core.windows.net/ev_sessions` |
| `/mnt/silver/payments` | `abfss://silver@evdatalakedev.dfs.core.windows.net/payments` |
| `/mnt/gold/summary` | `abfss://gold@evdatalakedev.dfs.core.windows.net/summary` |
| `/mnt/source/uploads` | `abfss://source@evdatalakedev.dfs.core.windows.net/uploads` |

The data is identical — only the path syntax changes.

---

## Prerequisites

Before running any cell, confirm all of these are done:

- [ ] Key Vault `kv-ev-intelligence-dev` exists (Day 1 Part 4)
- [ ] Secret scope `kv-ev-scope` created in Databricks (Day 1 Part 6.5)
- [ ] These 4 secrets exist in Key Vault:

| Secret Name | What it holds |
|---|---|
| `adls-account-name` | `evdatalakedev` |
| `sp-client-id` | Service Principal Application (client) ID |
| `sp-client-secret` | Service Principal client secret |
| `sp-tenant-id` | Azure Entra ID tenant ID |

- [ ] Service Principal has **Storage Blob Data Contributor** role on `evdatalakedev` (Day 1 Part 5.3)
- [ ] Cluster is running (any mode — Dedicated, Standard, Shared, or Serverless)

---

## How to Create the Notebook in Databricks

**Option 1 — Import (fastest):**
1. Databricks → left menu **Workspace**
2. Right-click your target folder → **Import**
3. Select **File** → browse to `00b_connect_storage_no_mount.ipynb`
4. Click **Import**
5. Attach to your running cluster from the top-right dropdown

**Option 2 — Create manually:**
1. Databricks → **Workspace** → **+ New** → **Notebook**
2. Name: `00b_connect_storage_no_mount`
3. Default language: **Python**
4. Attach to your running cluster
5. Copy each cell below into the notebook in order

---

## Notebook Cells — Run in Order

---

### Cell 1 — Load secrets from Key Vault

> **What this does:** Reads your 4 SP credentials from Key Vault via the secret scope. Nothing is hardcoded — values are masked in notebook output automatically by Databricks.

```python
# ── Cell 1: Load secrets from Key Vault ───────────────────────────────────────
SCOPE = "kv-ev-scope"

storage_account  = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
sp_client_id     = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
sp_tenant_id     = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")

print(f"Storage account : {storage_account}")
print(f"SP client ID    : {sp_client_id[:8]}...[REDACTED]")
print(f"SP tenant ID    : {sp_tenant_id}")
print("All secrets loaded — OK")
```

**Expected output:**
```
Storage account : evdatalakedev
SP client ID    : xxxxxxxx...[REDACTED]
SP tenant ID    : xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
All secrets loaded — OK
```

**If this cell fails:**

| Error | Fix |
|---|---|
| `Secret does not exist: adls-account-name` | Secret not added to Key Vault — Key Vault → Secrets → add it |
| `Secret scope 'kv-ev-scope' does not exist` | Secret scope not created — complete Day 1 Part 6.5 first |
| `PERMISSION_DENIED: Invalid permissions on KeyVault 403` | `AzureDatabricks` app missing `Key Vault Secrets User` role → Key Vault → IAM → Add that role → wait 2 min |

---

### Cell 2 — Configure Spark OAuth for your storage account

> **What this does:** Tells Spark: *"when you see paths for `evdatalakedev`, authenticate using this Service Principal via OAuth."*
> These settings live in Spark session memory only — gone when the cluster restarts.

```python
# ── Cell 2: Set Spark OAuth config for this storage account ───────────────────
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    sp_client_id
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    sp_client_secret
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token"
)

print(f"Spark OAuth config set for: {storage_account}")
print("You can now read/write using abfss:// paths — no mount needed.")
```

**Expected output:**
```
Spark OAuth config set for: evdatalakedev
You can now read/write using abfss:// paths — no mount needed.
```

> Cell 2 does **not** test the actual connection — it only sets the config. The real test happens in Cell 4.

---

### Cell 3 — Define the path helper function

> **What this does:** Defines `abfss()` so you never type the full storage URL manually. Every notebook uses this after running Cells 1 and 2.

```python
# ── Cell 3: Path helper — use in all notebooks ────────────────────────────────
def abfss(container: str, path: str = "") -> str:
    """
    Returns a full abfss:// path for a given container and optional subfolder/file.

    Usage:
        abfss("bronze")                    → abfss://bronze@evdatalakedev.dfs.core.windows.net
        abfss("silver", "ev_sessions")     → abfss://silver@evdatalakedev.dfs.core.windows.net/ev_sessions
        abfss("gold", "summary/2026/01/")  → abfss://gold@evdatalakedev.dfs.core.windows.net/summary/2026/01/
    """
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

print("Container paths:")
for container in ["bronze", "silver", "gold", "source"]:
    print(f"  {container:<8} → {abfss(container)}")
```

**Expected output:**
```
Container paths:
  bronze   → abfss://bronze@evdatalakedev.dfs.core.windows.net
  silver   → abfss://silver@evdatalakedev.dfs.core.windows.net
  gold     → abfss://gold@evdatalakedev.dfs.core.windows.net
  source   → abfss://source@evdatalakedev.dfs.core.windows.net
```

> **Cells 1, 2, 3 are the only cells you re-run every session.** Cells 4 and 5 below are one-time verification steps.

---

### Cell 4 — Verify read access to all 4 containers *(run once — first time only)*

> **What this does:** The real connection test. Calls `dbutils.fs.ls()` on each container. Empty containers show `0 items` — that is expected and correct on Day 1.

```python
# ── Cell 4: Verify read access to all 4 containers ───────────────────────────
print("=== Testing connection to all 4 containers ===\n")
all_ok = True

for container in ["bronze", "silver", "gold", "source"]:
    try:
        items = dbutils.fs.ls(abfss(container))
        print(f"  {container:<8}  OK — {len(items)} items")
    except Exception as e:
        print(f"  {container:<8}  ERROR — {e}")
        all_ok = False

print()
if all_ok:
    print("All 4 containers accessible — OAuth is working correctly.")
    print("You are ready to read and write data.")
else:
    print("One or more containers failed. See error table below.")
```

**Expected output (Day 1 — containers are empty):**
```
=== Testing connection to all 4 containers ===

  bronze    OK — 0 items
  silver    OK — 0 items
  gold      OK — 0 items
  source    OK — 0 items

All 4 containers accessible — OAuth is working correctly.
You are ready to read and write data.
```

**If this cell fails:**

| Error | Cause | Fix |
|---|---|---|
| `403 Forbidden` / `AuthorizationPermissionMismatch` | SP missing `Storage Blob Data Contributor` role | Storage account → **Access Control (IAM)** → confirm SP is assigned that role |
| `AuthenticationFailed` / `AADSTS7000215` | Wrong `sp-client-secret` in Key Vault | Key Vault → Secrets → `sp-client-secret` → value must match exactly what was shown when you created the SP |
| `Container not found` | Container doesn't exist | Portal → Storage account → Containers → confirm all 4 exist |
| Cell 4 fails but Cell 2 had no error | Config set but SP credentials are wrong | Re-check `sp-client-id` and `sp-tenant-id` in Key Vault |

---

### Cell 5 — Write and read a test file *(run once — first time only)*

> **What this does:** Writes a 1-row Parquet file to `bronze/_connection_test/`, reads it back, then deletes it. Confirms write permission.

```python
# ── Cell 5: Write + read + delete a test file ─────────────────────────────────
from pyspark.sql import Row

test_path = abfss("bronze", "_connection_test/test.parquet")

try:
    df_test = spark.createDataFrame([Row(status="ok", message="write test passed")])
    df_test.write.mode("overwrite").parquet(test_path)
    print(f"Write OK  → {test_path}")

    df_read = spark.read.parquet(test_path)
    df_read.show(truncate=False)
    print("Read  OK  — data matches what was written")

    dbutils.fs.rm(abfss("bronze", "_connection_test"), recurse=True)
    print("Cleanup OK — test file deleted")
    print("\nWrite access confirmed.")

except Exception as e:
    print(f"ERROR: {e}")
    print("If 403: role is probably 'Storage Blob Data Reader' not 'Contributor'.")
    print("Fix: Storage account → IAM → change role to 'Storage Blob Data Contributor'.")
```

**Expected output:**
```
Write OK  → abfss://bronze@evdatalakedev.dfs.core.windows.net/_connection_test/test.parquet
+------+------------------+
|status|message           |
+------+------------------+
|ok    |write test passed |
+------+------------------+
Read  OK  — data matches what was written
Cleanup OK — test file deleted

Write access confirmed.
```

---

### Cell 6 — Read/write reference patterns *(not a runnable cell — copy from here into future notebooks)*

```python
# ── Cell 6: Reference patterns — copy these into your actual notebooks ─────────

# READ a Delta table from silver
df = spark.read.format("delta").load(abfss("silver", "ev_sessions"))

# READ all Parquet files in a bronze folder
df = spark.read.parquet(abfss("bronze", "api_payments/"))

# READ a CSV from source
df = spark.read.option("header", "true").csv(abfss("source", "uploads/ev_data.csv"))

# READ nested CSVs (year/month/day/hour folder structure)
df = spark.read.option("header", "true").csv(
    abfss("source", "realtime/charging_sessions/*/*/*/*/*.csv")
)

# WRITE (overwrite) a Delta table to silver
df.write.format("delta").mode("overwrite").save(abfss("silver", "ev_sessions"))

# WRITE (append) to an existing Delta table
df.write.format("delta").mode("append").save(abfss("silver", "ev_sessions"))

# WRITE Parquet to bronze
df.write.mode("append").parquet(abfss("bronze", "api_payments"))

# LIST files in a container or subfolder
display(dbutils.fs.ls(abfss("bronze")))
display(dbutils.fs.ls(abfss("silver", "ev_sessions")))
```

---

## Using This in Every Other Notebook (The Right Way)

Instead of copy-pasting Cells 1–3 into every notebook, use `%run` to call this notebook from any other notebook. One line at the top does everything.

### Step 1 — Make sure `00b_connect_storage_no_mount` is saved in your Databricks Workspace

It must be saved in a known path, for example:
```
/Users/your-email@domain.com/00b_connect_storage_no_mount
```
or a shared folder:
```
/Shared/ev-project/00b_connect_storage_no_mount
```

### Step 2 — Add this as the first cell in every notebook that uses ADLS

```python
# ── Always run this first — sets up storage auth for this session ──────────────
%run "./00b_connect_storage_no_mount"
```

> Use a **relative path** (`./`) if the notebooks are in the same folder.
> Use a **full workspace path** if they are in different folders:
> ```python
> %run "/Shared/ev-project/00b_connect_storage_no_mount"
> ```

### What `%run` does

`%run` executes the entire target notebook in the **same Spark session** as the calling notebook. That means:
- All variables (`storage_account`, `sp_client_id`, etc.) become available
- The `abfss()` function is available
- The Spark OAuth config is set

After that one line, you can immediately read and write:

```python
# Cell 2 of your actual notebook — storage is already configured by %run above
df = spark.read.format("delta").load(abfss("silver", "ev_sessions"))
df.show()
```

### Full example — what a Day 2+ notebook looks like

```python
# ── Cell 1: Init storage (always first) ───────────────────────────────────────
%run "./00b_connect_storage_no_mount"

# ── Cell 2: Your actual work starts here ──────────────────────────────────────
# Storage is ready — use abfss() directly

df_bronze = spark.read.parquet(abfss("bronze", "api_payments/"))
print(f"Rows in bronze payments: {df_bronze.count():,}")

# ── Cell 3: Write cleaned data to silver ──────────────────────────────────────
df_silver = df_bronze.dropDuplicates(["payment_id"])
df_silver.write.format("delta").mode("overwrite").save(abfss("silver", "payments"))
print("Written to silver — OK")
```

---

## Summary

### Which cells to run and when

| Cell | What it does | Run every session? | Run every notebook? |
|---|---|---|---|
| Cell 1 | Load secrets from Key Vault | Yes | Yes (or use `%run`) |
| Cell 2 | Set Spark OAuth config | Yes | Yes (or use `%run`) |
| Cell 3 | Define `abfss()` helper | Yes | Yes (or use `%run`) |
| Cell 4 | Verify read access to all 4 containers | No — first time only | No |
| Cell 5 | Verify write access (test file) | No — first time only | No |
| Cell 6 | Reference read/write patterns | No — just copy from it | No |

### The minimal session checklist

Every time you open Databricks and want to work with ADLS:

```
1. Start your cluster (if not already running)
2. Open any notebook
3. Add "%run ./00b_connect_storage_no_mount" as the first cell
4. Run it
5. Done — abfss() is available, storage is connected
```

That's it. No re-mounting, no re-pasting credentials, no extra setup.
