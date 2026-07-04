# 05 — Unity Catalog: Access Connector, Storage Credential, External Locations
**Day 2 | Step 5 of 5**

Connect your ADLS Gen2 storage to Unity Catalog so you can browse Bronze / Silver / Gold folders directly in the Databricks Catalog UI — and enforce fine-grained access control on storage paths.

This is what you already created (shown in the screenshot):
- `cred-ev-intelligence-dev` — Storage Credential (Managed Identity)
- `evdatalakedev-bronze` — External Location → `abfss://bronze@evdatalakedev.dfs.core.windows.net/`
- `evdatalakedev-silver` — External Location → `abfss://silver@evdatalakedev.dfs.core.windows.net/`
- `evdatalakedev-gold` — External Location → `abfss://gold@evdatalakedev.dfs.core.windows.net/`

This file documents every step so students can reproduce it from scratch.

---

## How the Pieces Connect

```
ADLS Gen2 Storage (evdatalakedev)
         │
         │  IAM role: Storage Blob Data Contributor
         ▼
Access Connector (ac-ev-intelligence-dev)
  — Azure resource with its own Managed Identity
         │
         │  registered as
         ▼
Storage Credential (cred-ev-intelligence-dev)
  — Unity Catalog object wrapping the Access Connector's identity
  — stores no keys or tokens — just a pointer to the Managed Identity
         │
         │  one External Location per container
         ▼
External Locations
  evdatalakedev-bronze  →  abfss://bronze@evdatalakedev.dfs.core.windows.net/
  evdatalakedev-silver  →  abfss://silver@evdatalakedev.dfs.core.windows.net/
  evdatalakedev-gold    →  abfss://gold@evdatalakedev.dfs.core.windows.net/
         │
         ▼
Volumes (optional — makes paths browsable in Catalog UI tree)
  bronze_volume / silver_volume / gold_volume
```

**Why Unity Catalog and not just `abfss://` paths?**

| Without Unity Catalog | With Unity Catalog |
|---|---|
| Access storage only by hardcoding `abfss://` paths in notebooks | Browse folders in Catalog UI like a file explorer |
| No centralised access control on storage paths | Fine-grained access control: grant/revoke per user or group |
| Any notebook can read any path if it has the SP config | Only users/groups with `READ FILES` privilege on an External Location can access it |
| No audit trail of who accessed what | Full audit trail in Databricks Unity Catalog |

---

## Glossary

| Term | Plain English |
|---|---|
| **Access Connector** | An Azure resource that acts as a bridge between Unity Catalog and ADLS Gen2. It has its own Managed Identity. Unity Catalog uses this identity to authenticate with storage — no keys or passwords involved. |
| **Storage Credential** | A Unity Catalog object that wraps the Access Connector's identity. You create it once and reference it from all External Locations. |
| **External Location** | A Unity Catalog object that maps a name to an `abfss://` path. Once registered, Unity Catalog can govern who can read or write that path. |
| **Volume** | A Unity Catalog object that makes an External Location path browsable inside the Catalog tree. Pure metadata — no data is moved. |
| **Managed Identity** | An automatically managed Azure identity. No password to rotate. You assign it IAM roles just like a Service Principal. |

---

## Part 1 — Create the Access Connector (Azure side) (10 min)

> **Cost: ₹0** — Access Connectors are free.

**What is the Access Connector?**
It is an Azure resource that provides a Managed Identity for Unity Catalog to use. Unity Catalog Storage Credentials only support Managed Identity — not Service Principal client secrets or SAS tokens. The Access Connector is the only supported way to connect Unity Catalog to ADLS Gen2 in Azure.

### 1.1 Via Azure Portal

1. Go to [https://portal.azure.com](https://portal.azure.com)
2. In the top search bar, type **Access Connector for Azure Databricks** → click it
3. Click **+ Create**
4. Fill in:
   - **Subscription:** your subscription
   - **Resource group:** `rg-ev-intelligence-dev`
   - **Name:** `ac-ev-intelligence-dev`
   - **Region:** `Central India`
   - **Managed identity:** System assigned
5. Click **Review + Create** → **Create**
6. Wait ~30 seconds → click **Go to resource**
7. On the Overview page → left menu → **Identity**
8. Copy the **Object (principal) ID** — you need this for Part 2

> **Wait 1–2 minutes after creation** before assigning IAM roles — the Managed Identity needs time to propagate in Azure Entra ID.

### 1.2 Via CLI

> **CMD / PowerShell users:** The `\` line continuation below is bash syntax and will break in CMD/PowerShell. Use the single-line version to copy-paste directly.

**Single line (CMD / PowerShell):**
```cmd
az databricks access-connector create --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --location centralindia --identity-type SystemAssigned
```

**Multi-line (bash / Git Bash only):**
```bash
az databricks access-connector create \
  --name ac-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --location centralindia \
  --identity-type SystemAssigned
```

**Get the principal ID (CMD / PowerShell):**
```cmd
az databricks access-connector show --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query "identity.principalId" -o tsv
```
Copy the output — this is your `AC_PRINCIPAL_ID`.

> **If CLI gives `'databricks' is not in the 'az' command group`:**
> Install the extension first: `az extension add --name databricks` then retry.

---

## Part 2 — Assign IAM Role to the Access Connector (10 min)

> **Cost: ₹0** — RBAC role assignments are free.

The Access Connector's Managed Identity needs `Storage Blob Data Contributor` on your storage account. Without this, all External Location tests fail with `403 Forbidden`.

**Roles required:**

| Role | On | Required? |
|---|---|---|
| `Storage Blob Data Contributor` | `evdatalakedev` | Yes — needed for read/write/list/delete |
| `Storage Account Contributor` | `evdatalakedev` | Optional — only for File Events |
| `EventGrid EventSubscription Contributor` | Resource group | Optional — only for File Events |
| `Storage Queue Data Contributor` | `evdatalakedev` | Optional — only for File Events |

> **File Events** = instant notifications when a file arrives in storage (instead of Databricks polling the folder). Useful for Auto Loader. For dev, skip the last 3 optional roles — Read/Write/List/Delete all work without them. If External Location creation shows a yellow "File Events failed" warning, click **Force create** — everything works, just without instant file notifications.

### 2.1 Via Azure Portal

1. Portal → **Storage accounts** → `evdatalakedev`
2. Left menu → **Access Control (IAM)**
3. Click **+ Add** → **Add role assignment**
4. **Role** tab: search `Storage Blob Data Contributor` → select → **Next**
5. **Members** tab:
   - **Assign access to:** `Managed identity`
   - Click **+ Select members**
   - **Managed identity** dropdown: select `Access Connector for Azure Databricks`
   - Select `ac-ev-intelligence-dev` → **Select**
6. Click **Review + assign** → **Review + assign**
7. Wait **2 minutes** before moving to Part 3

### 2.2 Via CLI

**Step 1 — Get the Access Connector principal ID (if not done in Part 1):**
```cmd
az databricks access-connector show --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query "identity.principalId" -o tsv
```
Copy the output → `AC_PRINCIPAL_ID`

**Step 2 — Get the storage account resource ID:**
```cmd
az storage account show --name evdatalakedev --resource-group rg-ev-intelligence-dev --query id -o tsv
```
Copy the output → `STORAGE_ID`

**Step 3 — Assign the role (CMD / PowerShell):**
```cmd
az role assignment create --assignee-object-id <AC_PRINCIPAL_ID> --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" --scope <STORAGE_ID>
```

**Multi-line (bash / Git Bash only):**
```bash
AC_PRINCIPAL_ID=$(az databricks access-connector show \
  --name ac-ev-intelligence-dev \
  --resource-group rg-ev-intelligence-dev \
  --query "identity.principalId" -o tsv)

STORAGE_ID=$(az storage account show \
  --name evdatalakedev \
  --resource-group rg-ev-intelligence-dev \
  --query id -o tsv)

az role assignment create \
  --assignee-object-id $AC_PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

**Verify (CMD / PowerShell):**
```cmd
az role assignment list --scope <STORAGE_ID> --query "[?principalName=='ac-ev-intelligence-dev'].{Role:roleDefinitionName, Principal:principalName}" -o table
```

---

## Part 3 — Create Storage Credential in Unity Catalog (10 min)

> **Cost: ₹0** — Storage Credentials are free Unity Catalog metadata objects.

**What is a Storage Credential?**
A Unity Catalog object that tells Databricks: "when you need to access storage, use the Managed Identity of `ac-ev-intelligence-dev`." It stores no keys — just a pointer to the Access Connector's resource ID.

**Get the Access Connector resource ID — you need this for the credential:**

**CMD / PowerShell:**
```cmd
az databricks access-connector show --name ac-ev-intelligence-dev --resource-group rg-ev-intelligence-dev --query id -o tsv
```

The output looks like:
```
/subscriptions/81dd57e1-876a-4fcc-8778-e06f68c13228/resourceGroups/rg-ev-intelligence-dev/providers/Microsoft.Databricks/accessConnectors/ac-ev-intelligence-dev
```
Copy this — you will paste it in the next step.

### 3.1 Via Databricks UI

1. Databricks → left menu → **Catalog** (grid icon)
2. At the top of the Catalog pane → click **External Data** → **Storage Credentials**
3. Click **+ Create credential**
4. Fill in:
   - **Credential type:** `Azure Managed Identity`

   > You will see other options: AWS IAM Role, Cloudflare API Token. Do NOT select these — they are for other cloud providers.

   - **Credential name:** `cred-ev-intelligence-dev`
   - **Access Connector ID:** paste the full resource ID from the CLI command above
   - **Managed Identity ID:** leave blank (only needed for user-assigned MIs)
5. Click **Create**
6. The credential appears in the list — status should show green

> **If you see `Access Connector not found`:**
> The resource ID has a typo. Re-run the CLI command to get the exact ID and paste it again — it is case-sensitive.

> **If creation fails with `Permission denied`:**
> Your Databricks account needs the `Account admin` role. Go to `accounts.azuredatabricks.net` → User management → confirm you are Account admin.

---

## Part 4 — Create External Locations (one per container) (15 min)

> **Cost: ₹0** — External Locations are free Unity Catalog metadata objects.

**What is an External Location?**
A Unity Catalog object that maps a friendly name to an `abfss://` path. Once registered, Unity Catalog can enforce access control on that path — who can read, write, or list files there.

**Why `abfss://` and not `wasbs://`?**
Unity Catalog only works with `abfss://` (ADLS Gen2 with OAuth). `wasbs://` uses SAS tokens or account keys which Unity Catalog does not support. Source blob access (`dataenggdailystorage`) stays in notebooks using `wasbs://` — it does not get an External Location.

### 4.1 Via Databricks UI — Create bronze External Location

1. Databricks → **Catalog** → **External Data** → **External Locations**
2. Click **+ Create location** → **Create location manually**
3. Fill in:
   - **External location name:** `evdatalakedev-bronze`
   - **URL:** `abfss://bronze@evdatalakedev.dfs.core.windows.net/`
   - **Storage credential:** select `cred-ev-intelligence-dev`
4. Click **Create**
5. An automatic test runs — expected results:

   | Test | Expected |
   |---|---|
   | Read | ✅ Success |
   | List | ✅ Success |
   | Write | ✅ Success |
   | Delete | ✅ Success |
   | Path Exists | ✅ Success |
   | Hierarchical Namespace Enabled | ✅ Success |
   | File Events | ⚠️ May show Failed — this is OK for dev |

6. If File Events shows Failed → click **Force create the location** — Read/Write/List/Delete all work, File Events is optional
7. If all tests including File Events passed → click **Create** normally

**Repeat for silver and gold:**

| External location name | URL |
|---|---|
| `evdatalakedev-silver` | `abfss://silver@evdatalakedev.dfs.core.windows.net/` |
| `evdatalakedev-gold` | `abfss://gold@evdatalakedev.dfs.core.windows.net/` |

### 4.2 Via Databricks CLI (optional)

```cmd
databricks external-locations create --name evdatalakedev-bronze --url "abfss://bronze@evdatalakedev.dfs.core.windows.net/" --credential-name cred-ev-intelligence-dev
databricks external-locations create --name evdatalakedev-silver --url "abfss://silver@evdatalakedev.dfs.core.windows.net/" --credential-name cred-ev-intelligence-dev
databricks external-locations create --name evdatalakedev-gold   --url "abfss://gold@evdatalakedev.dfs.core.windows.net/"   --credential-name cred-ev-intelligence-dev
```

**Verify all 3 External Locations:**
1. Databricks → **Catalog** → **External Data** → **External Locations**
2. You should see all 3 listed with status Active
3. Click any location → **Test connection** to re-run the test anytime

---

## Part 5 — Create Volumes (make storage browsable in Catalog UI) (10 min)

> **Cost: ₹0** — Volumes are pure metadata. No data is copied or moved.

**What is a Volume?**
A Volume creates a browsable folder shortcut in the Catalog tree. After creating a volume, you can expand `Catalog → your_catalog → schema → volume_name` and browse your ADLS files like a file explorer — without writing any `abfss://` paths.

### Step 1 — Create schemas

You need one schema per container (schema = database in Unity Catalog).

Run in a Databricks SQL cell or `%sql` in a Python notebook:
```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

### Step 2 — Create Volumes via UI

1. **Catalog** → expand your catalog → expand `bronze` schema
2. Click **+** → **Create volume**
3. Fill in:
   - **Volume name:** `bronze_volume`
   - **Volume type:** `External`

   > **External vs Managed volume:**
   > - External: points to your existing ADLS path — use this
   > - Managed: Databricks controls the storage path — for temporary data only

   - **External location:** select `evdatalakedev-bronze`
   - **Path:** leave blank (root of the container)
4. Click **Create**

Repeat for silver and gold:

| Volume name | Schema | External location |
|---|---|---|
| `bronze_volume` | `bronze` | `evdatalakedev-bronze` |
| `silver_volume` | `silver` | `evdatalakedev-silver` |
| `gold_volume` | `gold` | `evdatalakedev-gold` |

### Step 2 — Create Volumes via SQL (faster — run all at once)

```sql
CREATE EXTERNAL VOLUME IF NOT EXISTS bronze.bronze_volume
  LOCATION 'abfss://bronze@evdatalakedev.dfs.core.windows.net/';

CREATE EXTERNAL VOLUME IF NOT EXISTS silver.silver_volume
  LOCATION 'abfss://silver@evdatalakedev.dfs.core.windows.net/';

CREATE EXTERNAL VOLUME IF NOT EXISTS gold.gold_volume
  LOCATION 'abfss://gold@evdatalakedev.dfs.core.windows.net/';
```

**Verify volumes are browsable:**
1. Catalog tree → expand your catalog → expand `bronze` → expand `bronze_volume`
2. You should see the folders inside the bronze container
3. Click any folder to browse files

---

## Part 6 — Verify Everything via Notebook

Run this in a new Databricks notebook to confirm the full setup is working:

```python
print("=== External Locations ===")
locations = spark.sql("SHOW EXTERNAL LOCATIONS").collect()
for loc in locations:
    print(f"  {loc['name']:<30} → {loc['url']}")
```

```python
print("=== Volumes — browse via /Volumes/ path ===")
for container in ["bronze", "silver", "gold"]:
    try:
        items = dbutils.fs.ls(f"/Volumes/bronze/{container}_volume/")
        print(f"  {container}_volume : {len(items)} items found")
    except Exception as e:
        print(f"  {container}_volume : ERROR — {e}")
```

```python
print("=== Write/Read/Delete test via External Location ===")
test_path = "abfss://bronze@evdatalakedev.dfs.core.windows.net/_uc_test.txt"
try:
    dbutils.fs.put(test_path, "uc external location write test", overwrite=True)
    content = dbutils.fs.head(test_path)
    dbutils.fs.rm(test_path)
    print(f"  Write → Read → Delete : OK")
    print(f"  Content read back     : {content}")
except Exception as e:
    print(f"  ERROR: {e}")
```

---

## Permission Summary

| Permission | Assigned to | On | Why |
|---|---|---|---|
| `Storage Blob Data Contributor` | Access Connector Managed Identity | `evdatalakedev` | Read + write + list + delete files in ADLS Gen2 — required for all External Location tests to pass |
| `Storage Account Contributor` | Access Connector Managed Identity | `evdatalakedev` | File Events (optional) |
| `EventGrid EventSubscription Contributor` | Access Connector Managed Identity | Resource group | File Events (optional) |
| `Storage Queue Data Contributor` | Access Connector Managed Identity | `evdatalakedev` | File Events (optional) |

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| External Location test: all checks fail with 403 | Access Connector missing `Storage Blob Data Contributor` on `evdatalakedev` | Part 2 — assign the role, wait 2 min, re-test |
| External Location test: Read/Write ✅ but File Events ❌ | Access Connector missing the 3 optional EventGrid/Queue roles | Click **Force create** — location works fine without File Events. Add optional roles later if needed. |
| `Access Connector not found` when creating Storage Credential | Resource ID has a typo | Re-run: `az databricks access-connector show ... --query id -o tsv` and paste exact output |
| `Permission denied` creating Storage Credential | Databricks user is not Account admin | Go to `accounts.azuredatabricks.net` → User management → confirm Account admin role |
| Cannot find `ac-ev-intelligence-dev` in IAM member search | Access Connector Managed Identity not yet propagated | Wait 2–3 minutes after creating the Access Connector, then search again |
| Volume creation fails with `PERMISSION_DENIED` | User lacks `CREATE VOLUME` on the schema | `GRANT CREATE VOLUME ON SCHEMA bronze TO 'your-email@domain.com'` |
| `'databricks' is not in the 'az' command group` | Databricks CLI extension not installed | `az extension add --name databricks` |
