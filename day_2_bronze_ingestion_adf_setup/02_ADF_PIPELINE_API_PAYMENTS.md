# 02 — ADF Pipeline: API Payments → Bronze JSON
**Day 2 | Step 2 of 4**

Build an ADF pipeline that:
1. Logs in to VoltGrid API → gets a token
2. On first run: loads ALL payments pages (full load)
3. On subsequent runs: loads only pages where `updated_at > last_watermark` (incremental)
4. Writes every page as a JSON file to `bronze/api/payments/raw/ingestion_date={date}/` in ADLS Gen2
5. Writes the new high-watermark to `pipeline_audit` after a successful run

---

## Pipeline Overview

```
pl_bronze_api_payments
│
├── Step 1: Web Activity (×3)    Fetch base URL, username, password from Key Vault
│
├── Step 2: Web Activity         POST /api/auth/login/ → get token
│
├── Step 3: Set Variable         Store token in pipeline variable v_token
│
├── Step 4: Set Variable         Set watermark = "1900-01-01T00:00:00Z" (full) or last run value
│
├── Step 5: Until Activity       Loop while v_current_page <= v_total_pages
│   └── Step 5a: Copy Activity   GET /api/db/payments/?updated_after={wm}&page={n}
│                                → write each page as JSON to bronze/api/payments/raw/ingestion_date={date}/page_{n}.json
│   └── Step 5b: Set Variable    Increment v_current_page by 1
│
└── Step 6: Notebook Activity    Write new watermark to pipeline_audit table
```

---

## API Behaviour

```
GET /api/db/payments/?updated_after=2026-01-01T00:00:00Z&page=1&page_size=100

Response:
{
  "data": [ { ...payment... }, ... ],
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total": 12500,
    "total_pages": 125
  }
}
```

- Records are under `"data"` — NOT `"results"`. This is a common mistake with other frameworks.
- `updated_after` — ISO 8601 timestamp. Returns only rows where `updated_at > value`. Omit entirely on first run to get all rows.
- `page_size` max is 100.
- Loop pages 1 → `total_pages` to get everything.

---

## Part A — Create Datasets

### Dataset 1: VoltGrid Payments REST Source (`ds_voltgrid_payments_src`)

**UI Steps:**

1. ADF Studio → **Author** (pencil icon) → **Datasets** → **+ New dataset**
2. Search `REST` → **REST** → **Continue**
3. Fill in:
   - **Name:** `ds_voltgrid_payments_src`
   - **Linked service:** `ls_voltgrid_api`
   - **Relative URL:** `/api/db/payments/`
4. Click **OK**
5. Go to **Parameters** tab → **+ New** — add these 3 parameters:
   - `p_page` | Type: Int | Default: 1
   - `p_page_size` | Type: Int | Default: 100
   - `p_updated_after` | Type: String | Default: (leave empty)
6. Go to **Connection** tab → **Relative URL** field → click **Add dynamic content** and paste:
   ```
   /api/db/payments/?page=@{dataset().p_page}&page_size=@{dataset().p_page_size}@{if(empty(dataset().p_updated_after),'',concat('&updated_after=',dataset().p_updated_after))}
   ```
   > The `if(empty(...))` expression: on first run (watermark = empty string or "1900-..."), omit `updated_after` entirely → gets all rows. On subsequent runs, append the filter.

7. Click **Publish all**

---

### Dataset 2: Bronze Payments JSON Sink (`ds_bronze_payments_sink`)

**Why JSON?**
ADF's Copy Activity has no Delta format option. JSON is the best Bronze sink for API data because it stores the API response exactly as received — no flattening, no data loss, no schema assumptions. The Databricks notebook (`03_bronze_api_payments.ipynb`) reads these JSON files and writes them as a proper Delta table.

**Why not CSV?**
CSV flattens the structure and can silently drop nested fields or mishandle special characters in values. JSON preserves the original API response exactly — this is what "store as-is in Bronze" means.

**Flow:**
```
ADF Copy Activity
  → bronze/api/payments/raw/ingestion_date=2026-07-04/page_001.json   (raw JSON, one file per page)
  → bronze/api/payments/raw/ingestion_date=2026-07-04/page_002.json
  → ...
                                ↓
Databricks notebook
  → bronze/api/payments/delta/  (Delta table, deduplicated by payment_id in Silver)
```

Partitioning by `ingestion_date` means each daily run writes to its own folder — no file overwrites, clean separation of full load vs incremental runs.

**UI Steps:**

1. **Datasets** → **+ New dataset**
2. Search `Azure Data Lake Storage Gen2` → **Continue**
3. Search `JSON` → select **JSON** → **Continue**
4. Fill in:
   - **Name:** `ds_bronze_payments_sink`
   - **Linked service:** `ls_adls_bronze`
   - **File path (Container):** `bronze`
   - **File path (Directory):** click **Add dynamic content** → paste:
     ```
     api/payments/raw/ingestion_date=@{formatDateTime(utcNow(),'yyyy-MM-dd')}
     ```
   - **File path (File):** click **Add dynamic content** → paste:
     ```
     page_@{padStart(string(variables('v_current_page')),3,'0')}.json
     ```
5. **Connection** tab:
   - **File pattern:** `Set of objects`
   - **Encoding:** `UTF-8`
   - **Compression type:** None
6. Click **OK**
7. Click **Publish all**

> ADF writes one JSON file per page, e.g. `page_001.json`, `page_002.json`. The file name uses zero-padded page number so files sort correctly (001, 002 ... 125). The Databricks notebook reads all JSON files from `raw/` and writes Delta to `bronze/api/payments/delta/`.

---

## Part B — Create Pipeline `pl_bronze_api_payments`

**UI Steps:**

1. **Author** → **Pipelines** → **+ New pipeline**
2. **Name:** `pl_bronze_api_payments`
3. Go to **Parameters** tab → **+ New**:
   - `p_load_type` | Type: String | Default: `incremental`
4. Go to **Variables** tab → **+ New** — add these 5 variables:
   - `v_token` | Type: String
   - `v_watermark` | Type: String
   - `v_current_page` | Type: Integer | Default: 1
   - `v_total_pages` | Type: Integer | Default: 1
   - `v_ingestion_date` | Type: String

---

### Step 1 — Three Web Activities: Fetch secrets from Key Vault

ADF reads secrets at runtime using its Managed Identity. Add one Web Activity per secret.

**Web Activity 1: `act_get_base_url`**
- **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-api-base-url/?api-version=7.0`
- **Method:** GET
- **Authentication:** Managed Identity
- **Resource:** `https://vault.azure.net`
- Output used as: `@{activity('act_get_base_url').output.value}`

**Web Activity 2: `act_get_username`**
- **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-username/?api-version=7.0`
- **Method:** GET
- **Authentication:** Managed Identity
- **Resource:** `https://vault.azure.net`

**Web Activity 3: `act_get_password`**
- **URL:** `https://kv-ev-intelligence-dev.vault.azure.net/secrets/voltgrid-password/?api-version=7.0`
- **Method:** GET
- **Authentication:** Managed Identity
- **Resource:** `https://vault.azure.net`

Connect all 3 in sequence (1 → 2 → 3), or run 2 and 3 in parallel after 1.

---

### Step 2 — Web Activity: Login and get token

1. Add a 4th **Web** activity after the secret-fetch activities
2. **Name:** `act_api_login`
3. **Settings** tab:
   - **URL** (dynamic content):
     ```
     @{concat(activity('act_get_base_url').output.value, '/api/auth/login/')}
     ```
   - **Method:** POST
   - **Headers:** add one header:
     - Name: `Content-Type`
     - Value: `application/json`
   - **Body** (dynamic content):
     ```
     @{concat('{"username":"', activity('act_get_username').output.value, '","password":"', activity('act_get_password').output.value, '"}')}
     ```
4. Output of this activity: `@{activity('act_api_login').output.token}`

---

### Step 3 — Set Variable: Store token

1. Add **Set Variable** activity after `act_api_login`
2. **Name:** `act_set_token`
3. **Variable:** `v_token`
4. **Value** (dynamic content): `@{activity('act_api_login').output.token}`

---

### Step 4 — Set Variable: Set watermark

For Day 2, use a fixed watermark value. In Day 8 (ADF Orchestration) you will wire this to read from the real `pipeline_audit` table.

1. Add **Set Variable** activity after `act_set_token`
2. **Name:** `act_set_watermark`
3. **Variable:** `v_watermark`
4. **Value:** `1900-01-01T00:00:00Z`

> Setting to `1900-01-01T00:00:00Z` means every run fetches all records (full load). To simulate an incremental run: change this value to any recent date, re-run the pipeline, and you will see far fewer records copied.

---

### Step 4b — Set Variable: Set ingestion date

This captures today's date once at pipeline start — all pages written in this run share the same `ingestion_date` folder, keeping the run atomic.

1. Add **Set Variable** activity after `act_set_watermark`
2. **Name:** `act_set_ingestion_date`
3. **Variable:** `v_ingestion_date`
4. **Value** (dynamic content): `@{formatDateTime(utcNow(),'yyyy-MM-dd')}`

Connect: `act_set_watermark` → `act_set_ingestion_date`

---

### Step 5 — Until Activity: Paginate all pages

1. Add **Until** activity after `act_set_watermark`
2. **Name:** `act_paginate`
3. **Expression** (stop condition — loop stops when this is True):
   ```
   @greater(variables('v_current_page'), variables('v_total_pages'))
   ```

**Inside the Until, add two activities:**

#### 5a — Copy Activity: `act_copy_payments_page`

**Source tab:**
- Dataset: `ds_voltgrid_payments_src`
- Dataset parameters:
  - `p_page`: `@{variables('v_current_page')}`
  - `p_page_size`: `100`
  - `p_updated_after`: `@{variables('v_watermark')}`
- **Additional headers:**
  ```
  Authorization: Token @{variables('v_token')}
  ```
- **Pagination rules** → **+ New rule:**
  - Absolute URL: leave empty
  - Query parameter for next page: leave empty
  - Support RFC 5988: No
  - **JSON path expressions → Add rule:**
    - Name: anything (e.g. `totalPages`)
    - Value: `$.pagination.total_pages`
    - Variable: `v_total_pages`

> This pagination rule reads `total_pages` from the first response and stores it in `v_total_pages`. The Until loop uses that to know when to stop.

**Sink tab:**
- Dataset: `ds_bronze_payments_sink`
- **File name option:** `File name from dataset` (uses the dynamic `page_001.json` expression you set in the dataset)
- **Write behavior:** `Add dynamic content` is not needed here — the dataset file path already includes `v_current_page`

> **Note:** The dataset's File path (Directory) uses `formatDateTime(utcNow(),'yyyy-MM-dd')` — this resolves at runtime to today's date, e.g. `api/payments/raw/ingestion_date=2026-07-04/`. Every page in this pipeline run lands in the same dated folder.

**Mapping tab:**

Leave mapping as **Auto mapping** — do not add any column mappings.

JSON sink in ADF writes the full response body as-is. There is nothing to map — the entire JSON object from the API goes into the file unchanged. Column mapping is only for CSV/tabular sinks where you need to control which fields land in which column.

#### 5b — Set Variable: Increment page

1. Add **Set Variable** activity after `act_copy_payments_page` inside the Until
2. **Name:** `act_increment_page`
3. **Variable:** `v_current_page`
4. **Value** (dynamic content): `@{add(variables('v_current_page'), 1)}`

Connect: `act_copy_payments_page` → `act_increment_page` (on Success)

---

### Step 6 — Notebook Activity: Write watermark (optional for Day 2)

> This step is optional for Day 2. The watermark is written by the Databricks notebook (`03_bronze_api_payments.ipynb`) directly. You can skip the ADF Notebook Activity for now and run the Databricks notebook manually after the pipeline.

If you want ADF to trigger it automatically:

1. Add **Notebook** activity after the Until
2. **Name:** `act_write_audit`
3. **Azure Databricks** tab:
   - Linked service: `ls_databricks` (create this — see Part C below)
   - Notebook path: `/Shared/ev-project/03_bronze_api_payments`
4. **Base parameters:**
   - `pipeline_run_id`: `@{pipeline().RunId}`
   - `load_type`: `@{pipeline().parameters.p_load_type}`

---

## Part C — Create Databricks Linked Service `ls_databricks` (optional)

Only needed if you want Step 6 (Notebook Activity) in the pipeline.

### UI Steps

1. Manage → Linked services → **+ New**
2. Search `Azure Databricks` → **Continue**
3. Fill in:
   - **Name:** `ls_databricks`
   - **Azure subscription:** yours
   - **Databricks workspace:** `dbw-ev-intelligence-dev`
   - **Select cluster:** Existing interactive cluster → select `dev-cluster`
   - **Access token:** use Key Vault reference or enter a Personal Access Token

> To create a Databricks PAT: Databricks UI → top-right user menu → **Settings** → **Developer** → **Access tokens** → **Generate new token**.
> Store it as `databricks-pat` in Key Vault.

### CLI Steps

**Step 1 — Get Databricks workspace URL (CMD / PowerShell):**
```cmd
az databricks workspace show --resource-group rg-ev-intelligence-dev --name dbw-ev-intelligence-dev --query "workspaceUrl" -o tsv
```
Copy the output (e.g. `adb-1234567890.12.azuredatabricks.net`).

**Step 2 — Get Databricks workspace resource ID (CMD / PowerShell):**
```cmd
az databricks workspace show --resource-group rg-ev-intelligence-dev --name dbw-ev-intelligence-dev --query "id" -o tsv
```
Copy the output.

**Step 3 — Store PAT in Key Vault (CMD / PowerShell — replace `<PAT>` with your token):**
```cmd
az keyvault secret set --vault-name kv-ev-intelligence-dev --name "databricks-pat" --value "<PAT>"
```

**Step 4 — Create the linked service (CMD / PowerShell — replace `<workspace-url>` and `<workspace-resource-id>`):**
```cmd
az datafactory linked-service create --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --linked-service-name "ls_databricks" --properties "{\"type\": \"AzureDatabricks\", \"typeProperties\": {\"domain\": \"https://<workspace-url>\", \"accessToken\": {\"type\": \"AzureKeyVaultSecret\", \"store\": {\"referenceName\": \"ls_keyvault\", \"type\": \"LinkedServiceReference\"}, \"secretName\": \"databricks-pat\"}, \"existingClusterId\": \"<cluster-id>\"}}"
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory linked-service create \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --linked-service-name "ls_databricks" \
  --properties '{
    "type": "AzureDatabricks",
    "typeProperties": {
      "domain": "https://<workspace-url>",
      "accessToken": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "ls_keyvault",
          "type": "LinkedServiceReference"
        },
        "secretName": "databricks-pat"
      },
      "existingClusterId": "<cluster-id>"
    }
  }'
```

> To find `cluster-id`: Databricks UI → **Compute** → click `dev-cluster` → look in the URL: `https://adb-xxx.azuredatabricks.net/#setting/clusters/<cluster-id>/configuration`

---

## Part D — Trigger the Pipeline

### Manual trigger — UI

1. Open `pl_bronze_api_payments` in ADF Studio
2. Click **Add trigger** → **Trigger now**
3. `p_load_type`: `full`
4. Click **OK**
5. Go to **Monitor** tab → watch the pipeline run → all activities should show green

### Scheduled trigger — UI

1. **Add trigger** → **New/Edit**
2. **Name:** `tr_bronze_api_payments_daily`
3. **Type:** Schedule
4. **Recurrence:** every day at `02:00 UTC`
5. **Parameters:** `p_load_type` = `incremental`
6. Click **OK** → **Publish all**

### Manual trigger — CLI

> **CMD / PowerShell users:** Use the single-line version.

**Single line (CMD / PowerShell):**
```cmd
az datafactory pipeline create-run --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --pipeline-name "pl_bronze_api_payments" --parameters "{\"p_load_type\": \"full\"}"
```

**Multi-line (bash / Git Bash only):**
```bash
az datafactory pipeline create-run \
  --resource-group rg-ev-intelligence-dev \
  --factory-name adf-ev-intelligence-dev \
  --pipeline-name "pl_bronze_api_payments" \
  --parameters '{"p_load_type": "full"}'
```

**Check run status (CMD / PowerShell):**
```cmd
az datafactory pipeline-run query-by-factory --resource-group rg-ev-intelligence-dev --factory-name adf-ev-intelligence-dev --last-updated-after "2026-01-01T00:00:00Z" --last-updated-before "2027-01-01T00:00:00Z" --query "value[0].{Status:status, RunId:runId, Message:message}" -o table
```

---

## Verify in ADLS (Databricks)

After the pipeline runs, check JSON files landed correctly:

```python
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/"))
```

Expected output: one folder per run date, e.g. `ingestion_date=2026-07-04/`

```python
display(dbutils.fs.ls("abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/ingestion_date=2026-07-04/"))
```

Expected output: `page_001.json`, `page_002.json` ... one file per API page.

```python
df_raw = spark.read.option("multiLine", "true").json(
    "abfss://bronze@evdatalakedev.dfs.core.windows.net/api/payments/raw/ingestion_date=2026-07-04/"
)
print(f"Pages loaded: {df_raw.count()}")
display(df_raw.limit(3))
```

Expected: each row is one full API page response with `data` array and `pagination` object — exactly as the API returned it. The Databricks notebook (`03_bronze_api_payments.ipynb`) explodes the `data` array and writes individual payment records as a Delta table to `bronze/api/payments/delta/`.

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `401 Unauthorized` from login Web Activity | Username or password secret wrong in Key Vault | Check `voltgrid-username` and `voltgrid-password` values in Key Vault |
| `Until loop runs forever` | `v_total_pages` variable not updated from pagination rule | Check pagination rule — JSON path must be `$.pagination.total_pages` and variable must be `v_total_pages` |
| `Copy writes 0 rows` | Source REST dataset returning empty response | Check `p_updated_after` watermark — if too recent, no records exist; try with `1900-01-01T00:00:00Z` |
| `JSON write fails: 403` | ADF MI missing `Storage Blob Data Contributor` on `evdatalakedev` | Day 2 Part 2 — assign the role, wait 2 min, retry |
| `All page files named page_000.json` | `v_current_page` variable not incrementing | Check `act_increment_page` is connected on Success after `act_copy_payments_page` inside the Until |
| `Key Vault access denied` in Web Activity | ADF MI missing `Key Vault Secrets User` on Key Vault | Day 2 Part 3 — assign the role, wait 2 min, retry |
| `Notebook activity fails: cluster not running` | `dev-cluster` was terminated | Start `dev-cluster` first, or switch to Job cluster in `ls_databricks` |

---

## Next Step

→ `03_ADF_PIPELINE_BLOB_SESSIONS.md` — build the blob charging sessions pipeline
