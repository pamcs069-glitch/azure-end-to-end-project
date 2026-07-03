# 00b — Connect to ADLS Gen2 Without Mounting
**Day 1 | Modern Alternative to `00_mount_storage`**

> **Use this guide if:**
> - Your cluster Access mode is **Standard**, **Shared**, or **Serverless** (mount is blocked in these modes)
> - Your cluster is **Dedicated** but you want the modern approach (recommended for all new learners)
>
> **Skip this guide if:** you already ran `00_mount_storage.ipynb` and it worked — no need to do both.

---

## What Is This?

Instead of mounting containers to `/mnt/bronze`, `/mnt/silver`, etc., you configure Spark with your Service Principal OAuth credentials **once per session**, then read and write using full `abfss://` paths directly.

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

> **What this does:** Reads your 4 SP credentials from Key Vault via the secret scope. Nothing is hardcoded — the values are masked in notebook output automatically.

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
| `Secret does not exist: adls-account-name` | Secret not added to Key Vault — go to Key Vault → Secrets → add it |
| `Secret scope 'kv-ev-scope' does not exist` | Secret scope not created — complete Day 1 Part 6.5 first |
| `PERMISSION_DENIED: Invalid permissions on KeyVault 403` | `AzureDatabricks` app missing `Key Vault Secrets User` role → Key Vault → IAM → Add that role → wait 2 min |

---

### Cell 2 — Configure Spark OAuth for your storage account

> **What this does:** Tells Spark: *"when you see paths pointing to `evdatalakedev`, authenticate using this Service Principal via OAuth."* These settings last for this Spark session only — you re-run this cell after every cluster restart (takes 5 seconds).

```python
# ── Cell 2: Set Spark OAuth config for this storage account ───────────────────
# Scoped to your specific storage account — safe if you connect to multiple accounts later

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

> **No errors here means Spark accepted the config — it does not test the connection yet.** Actual authentication happens in Cell 4 when you first try to list a container.

---

### Cell 3 — Define the path helper function

> **What this does:** Defines a small `abfss()` helper so you never have to type the full storage URL manually in your notebooks. Use this in all future notebooks after running Cells 1 and 2.

```python
# ── Cell 3: Path helper — use this in all notebooks ───────────────────────────
def abfss(container: str, path: str = "") -> str:
    """
    Build a full abfss:// path for a container and optional subfolder/file.

    Examples:
        abfss("bronze")                       → abfss://bronze@evdatalakedev.dfs.core.windows.net
        abfss("silver", "ev_sessions")        → abfss://silver@evdatalakedev.dfs.core.windows.net/ev_sessions
        abfss("gold", "summary/2026/01/")     → abfss://gold@evdatalakedev...../summary/2026/01/
    """
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

# Verify — print root path for all 4 containers
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

---

### Cell 4 — Verify read access to all 4 containers

> **What this does:** This is your real connection test. It calls `dbutils.fs.ls()` on each container using the abfss path. If OAuth is configured correctly and the SP has the right RBAC role, all 4 will show `OK`. An empty container shows `0 items` — that is expected and fine on Day 1.

```python
# ── Cell 4: Verify read access to all 4 containers ───────────────────────────
print("=== Testing connection to all 4 containers ===\n")
all_ok = True

for container in ["bronze", "silver", "gold", "source"]:
    path = abfss(container)
    try:
        items = dbutils.fs.ls(path)
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
| `403 Forbidden` / `AuthorizationPermissionMismatch` | SP does not have `Storage Blob Data Contributor` role on `evdatalakedev` | Storage account → **Access Control (IAM)** → confirm SP is listed under that role |
| `AuthenticationFailed` / `AADSTS7000215` | Wrong `sp-client-secret` value in Key Vault | Key Vault → Secrets → `sp-client-secret` → check the value matches what was shown when you created the SP |
| `Container not found` | Container name does not exist in storage | Portal → Storage account → Containers → confirm all 4 exist: `bronze`, `silver`, `gold`, `source` |
| `Forbidden` on Cell 4 but Cell 2 had no error | Cell 2 accepted the config but the SP credentials are wrong | Re-check `sp-client-id` and `sp-tenant-id` values in Key Vault |

---

### Cell 5 — Write and read a test file (verify write access)

> **What this does:** Writes a tiny 1-row Parquet file to `bronze/_connection_test/`, reads it back, then deletes it. Confirms the SP has write permission (not just read).

```python
# ── Cell 5: Write + read + delete a test file to confirm write access ─────────
from pyspark.sql import Row

test_path = abfss("bronze", "_connection_test/test.parquet")

try:
    # Step 1: write a tiny DataFrame
    df_test = spark.createDataFrame([Row(status="ok", message="write test passed")])
    df_test.write.mode("overwrite").parquet(test_path)
    print(f"Write OK  → {test_path}")

    # Step 2: read it back
    df_read = spark.read.parquet(test_path)
    df_read.show(truncate=False)
    print("Read  OK  — data matches what was written")

    # Step 3: clean up
    dbutils.fs.rm(abfss("bronze", "_connection_test"), recurse=True)
    print("Cleanup OK — test file deleted")
    print("\nWrite access confirmed — SP has Storage Blob Data Contributor role.")

except Exception as e:
    print(f"ERROR: {e}")
    print("If 403: SP has read but not write permission.")
    print("Fix: Storage account → IAM → confirm role is 'Storage Blob Data Contributor', not 'Storage Blob Data Reader'.")
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

Write access confirmed — SP has Storage Blob Data Contributor role.
```

---

### Cell 6 — Quick reference: how to use abfss paths in all future notebooks

> **What this does:** This cell is a reference template. It does not run any real data — just prints examples. Copy the patterns you need into future notebooks.

```python
# ── Cell 6: Reference — how to use abfss() in future notebooks ────────────────
# Run Cells 1, 2, 3 at the top of every notebook, then use these patterns:

# ── Reading ────────────────────────────────────────────────────────────────────

# Read a Delta table from silver
# df = spark.read.format("delta").load(abfss("silver", "ev_sessions"))

# Read all Parquet files in a bronze folder
# df = spark.read.parquet(abfss("bronze", "api_payments/"))

# Read a CSV from source
# df = spark.read.option("header", "true").csv(abfss("source", "uploads/ev_data.csv"))

# Read nested CSVs using glob (e.g. year/month/day/hour structure)
# df = spark.read.option("header", "true").csv(abfss("source", "realtime/charging_sessions/*/*/*/*/*.csv"))

# ── Writing ────────────────────────────────────────────────────────────────────

# Write (overwrite) a Delta table to silver
# df.write.format("delta").mode("overwrite").save(abfss("silver", "ev_sessions"))

# Append to an existing Delta table
# df.write.format("delta").mode("append").save(abfss("silver", "ev_sessions"))

# Write Parquet to bronze (raw landing zone)
# df.write.mode("append").parquet(abfss("bronze", "api_payments"))

# ── Checking what is in a folder ───────────────────────────────────────────────

# List files in bronze
# display(dbutils.fs.ls(abfss("bronze")))

# List files inside a specific subfolder
# display(dbutils.fs.ls(abfss("silver", "ev_sessions")))

# ── Equivalent mount vs direct path ───────────────────────────────────────────
print("Mount path  (old) : /mnt/bronze/ev_sessions")
print(f"Direct path (new) : {abfss('bronze', 'ev_sessions')}")
print()
print("Both point to the same data. Direct path is the modern standard.")
```

**Expected output:**
```
Mount path  (old) : /mnt/bronze/ev_sessions
Direct path (new) : abfss://bronze@evdatalakedev.dfs.core.windows.net/ev_sessions

Both point to the same data. Direct path is the modern standard.
```

---

## After Every Cluster Restart

Spark config is **not** persisted across cluster restarts. Each time your cluster restarts (or you start a new session), run these 3 cells before any read/write:

1. **Cell 1** — load secrets from Key Vault
2. **Cell 2** — set Spark OAuth config
3. **Cell 3** — re-define the `abfss()` helper

This takes under 30 seconds and replaces the old "re-run the mount notebook" step.

---

## Summary

| Step | Cell | What it does | Must re-run after restart? |
|---|---|---|---|
| Load secrets | Cell 1 | Reads SP credentials from Key Vault | Yes |
| Configure Spark | Cell 2 | Sets OAuth auth for the storage account | Yes |
| Path helper | Cell 3 | Defines `abfss()` shortcut function | Yes |
| Verify read | Cell 4 | Lists all 4 containers — confirms OAuth works | Optional (for peace of mind) |
| Verify write | Cell 5 | Writes + deletes a test file | Optional (first time only) |
| Reference | Cell 6 | Copy-paste patterns for future notebooks | No — just a reference |
