# ADF → Databricks Notebook Integration
**How to call a Databricks notebook from Azure Data Factory**
**Target notebook:** `02_bronze_blob_all_entities_v2` (Day 6 — Blob to Bronze Volume)

---

## Error You Are Seeing

```
Failed to navigate the Databricks workspace.
Error: Provided access token does not have required scopes: workspace
```

**Root cause:**
The token exists and is valid, but it was created with limited API scopes that do not include `workspace`. ADF requires the `workspace` scope to browse and trigger notebook runs via the Databricks REST API.

## QUICKFIX — Add `workspace` Scope to Existing Token

If your token already exists (like the one named "For giving access to adf") and is missing `workspace`:

1. Databricks → **User Settings** → **Access tokens** tab
2. Find the token → click **Edit** (pencil) or the token name
3. In the **"Update token"** dialog → click inside the **API scope(s)** box
4. Type `workspace` → select it from the dropdown → it appears as a tag
5. Click **Update**

ADF needs **two scopes** — one to browse the workspace, one to trigger runs:

| Scope | What ADF uses it for |
|---|---|
| `workspace` | Browse notebook paths (`/api/2.0/workspace/list`) |
| `jobs` | Submit and monitor notebook runs (`/api/2.0/jobs/runs/submit`) |

You may hit them one at a time:
- Add only `workspace` → can browse notebooks, run fails with **"does not have required scopes: jobs"**
- Add both → fully working

**Before (broken):**
```
clusters   databricks-connect   sql   access-management
```

**After (working):**
```
clusters   databricks-connect   sql   access-management   workspace   jobs
```

No need to regenerate the token or update the ADF Linked Service — the same token now works immediately.

---

**Other reasons this error fires:**

1. The Linked Service was created with a token that expired (PAT has a max expiry of 90 days)
2. No Databricks Linked Service exists — ADF is using a default/wrong connection
3. The Linked Service was created using the wrong token type (e.g., AAD token without workspace scope)
4. The Databricks workspace URL in the Linked Service is wrong

**Fix: Create or update the ADF Linked Service using a fresh Databricks PAT (Personal Access Token)** — follow the steps below.

---

## How It Works — Architecture Overview

```
Azure Data Factory Pipeline
  └── Notebook Activity
        └── Databricks Linked Service (PAT token)
              └── Databricks Workspace
                    └── /Shared/bronze_ingestion/02_bronze_blob_all_entities_v2
                          └── Parameters: load_type = incremental
```

ADF does NOT run the notebook itself. ADF calls the Databricks REST API (`/api/2.0/jobs/runs/submit`) using the PAT token to trigger a one-off notebook run inside the workspace.

The notebook runs on the Databricks cluster — ADF waits for it to complete and reports success/failure.

---

## Step 1 — Generate a Databricks PAT (Personal Access Token)

A PAT is the simplest authentication method. It gives ADF workspace-level access.

1. Open **Databricks workspace** (`https://adb-<workspace-id>.azuredatabricks.net`)
2. Click your **username** (top right) → **User Settings**
3. Click **Access tokens** tab → **Generate new token**
4. Fill in:
   - **Comment:** `adf-linked-service-token`
   - **Lifetime (days):** `90` (maximum allowed)
   - **Scope:** `Other APIs`
   - **API scope(s):** add all of these tags: `workspace`, `jobs`, `clusters`
5. Click **Generate** → **COPY THE TOKEN NOW** (it is only shown once)

> If you skip selecting scopes, Databricks may create a token without `workspace` or `jobs` — ADF needs both to browse notebooks AND submit runs.

> Save this token somewhere safe — you will need it in Step 3.
> When it expires in 90 days, regenerate and update the Linked Service.

---

## Step 2 — Find Your Databricks Workspace URL

You need the exact workspace URL for the Linked Service.

1. In the Databricks browser tab, copy the URL from the address bar
2. It looks like: `https://adb-1234567890123456.7.azuredatabricks.net`
3. Use **only** the base URL — no path, no trailing slash

---

## Step 3 — Create the Databricks Linked Service in ADF

1. Open **Azure Data Factory Studio** (`https://adf.azure.com`) → select your factory (`adf-ev-intelligence-dev`)
2. Left sidebar → **Manage** (wrench icon) → **Linked services** → **+ New**
3. In the search box type `Databricks` → select **Azure Databricks** → click **Continue**

### Configure the Linked Service

| Field | Value |
|---|---|
| Name | `ls_databricks_workspace` |
| Azure subscription | Select your subscription |
| Databricks workspace | Select `dbw-ev-intelligence-dev` |
| Select cluster | `Existing interactive cluster` |
| Access token | Paste the PAT from Step 1 |
| Existing cluster ID | See Step 3a below |

### Step 3a — Find Your Cluster ID

1. In Databricks → left sidebar → **Compute**
2. Click `dev-cluster`
3. In the URL, copy the cluster ID: `adb-xxxx.azuredatabricks.net/#setting/clusters/`**`0612-123456-abc1234`**
4. Paste this value into the **Existing cluster ID** field in ADF

> Using an existing interactive cluster avoids cold-start delays (new cluster takes 5-8 minutes).
> `dev-cluster` must be **running** when ADF triggers the notebook.

4. Click **Test connection** — you should see `Connection successful`
   - If you see `access token does not have required scopes: workspace` → the token was copied incorrectly. Regenerate and try again.
5. Click **Create**

---

## Step 4 — Upload the Notebook to the Workspace

The ADF Notebook Activity needs the notebook to exist at a known path in the workspace.

1. Databricks → left sidebar → **Workspace** → **Shared**
2. Create folder if needed: **⋮** → **Create folder** → `bronze_ingestion`
3. **⋮** next to `bronze_ingestion` → **Import**
4. Select file: `day_6_blob_to_bronze_volume_migration/02_bronze_blob_all_entities_v2.ipynb`
5. Confirm the notebook appears at:
   ```
   /Shared/bronze_ingestion/02_bronze_blob_all_entities_v2
   ```

---

## Step 5 — Add a Notebook Activity to Your ADF Pipeline

1. ADF Studio → **Author** (pencil icon) → open or create your pipeline
2. In the **Activities** panel (left), expand **Databricks** → drag **Notebook** onto the canvas
3. Click the Notebook activity → configure the tabs below

### Azure Databricks tab

| Field | Value |
|---|---|
| Databricks linked service | `ls_databricks_workspace` |

### Settings tab

| Field | Value |
|---|---|
| Notebook path | `/Shared/bronze_ingestion/02_bronze_blob_all_entities_v2` |

Click the folder icon next to the path to browse and select the notebook.

### Base parameters tab

Click **+ New** and add:

| Name | Value |
|---|---|
| `load_type` | `incremental` |

> This passes `load_type=incremental` as a Databricks widget to the notebook.
> The notebook reads it with `dbutils.widgets.get("load_type")` in Cell 2.
> For a full load run, change this to `full` before triggering manually.

---

## Step 6 — Test the Connection End-to-End

1. ADF Studio → your pipeline → click **Debug** (top toolbar)
2. Watch the pipeline run — the Notebook activity should show status `In Progress` then `Succeeded`
3. If it fails, click the Notebook activity run → **Output** tab to see the error

### What each status means

| Status | Meaning |
|---|---|
| `In Progress` | ADF successfully called Databricks REST API — notebook is running |
| `Succeeded` | Notebook ran to completion with no exceptions in the final cell |
| `Failed` | Notebook raised an exception OR the ADF-Databricks connection failed |

---

## Step 7 — Add to a Trigger (Schedule)

1. ADF Studio → your pipeline → **Add trigger** → **New/Edit**
2. Click **+ New**
3. Fill in:
   - **Name:** `tr_bronze_blob_daily`
   - **Type:** `Schedule`
   - **Start date/time:** today at 06:00 UTC
   - **Recurrence:** `Every 1 Day`
4. Click **OK** → **OK** → **Publish All**

> After publishing, the trigger becomes active. ADF will call the Databricks notebook at 06:00 UTC daily.

---

## Access & Permissions Checklist

The error `Provided access token does not have required scopes: workspace` fires for **any** access failure — not just expired tokens. Run through this checklist if your token is still valid.

---

### Check 1 — Verify the Token is Actually Valid

In Databricks → **User Settings** → **Access tokens** tab:
- Is your token still listed? (not expired/revoked)
- Does the expiry date show a future date?

If it is expired or missing → go to [Step 1](#step-1--generate-a-databricks-pat-personal-access-token) and regenerate.

If it is valid → continue to Check 2.

---

### Check 2 — Verify the Workspace URL Matches the Token

The most common cause when the token is valid: **the URL in the Linked Service is wrong**.

A PAT is tied to ONE specific workspace. If the Linked Service URL points to a different workspace, the token is rejected.

1. In Databricks, copy the URL from your browser: `https://adb-1234567890.7.azuredatabricks.net`
2. ADF → Manage → Linked services → `ls_databricks_workspace` → **Edit**
3. Confirm the **Databricks workspace URL** field matches exactly — same `adb-<number>` ID
4. If different, fix the URL → **Test connection** → **Create** → **Publish All**

---

### Check 3 — Workspace Entitlement (User Must Be a Workspace Member)

The PAT user must be a member of the Databricks workspace (not just an Azure user).

1. Databricks → left sidebar → **Settings** (gear icon) → **Identity and Access** → **Users**
2. Confirm your user account (`hariom.s@simformsolutions.com`) is listed
3. If not listed:
   - Click **Add user**
   - Enter the email → select role **User** (minimum) or **Admin**
   - Click **Add**

> If the user is not in the workspace at all, their PAT is valid but rejected at the workspace gate — which produces exactly this error.

---

### Check 4 — Notebook Path Permission

ADF calls the Databricks REST API to run the notebook. The PAT user must have at least **Can Read** on the notebook.

1. Databricks → Workspace → navigate to `/Shared/bronze_ingestion/`
2. Right-click `02_bronze_blob_all_entities_v2` → **Permissions**
3. Confirm one of these is set:

| Who | Permission needed |
|---|---|
| Your user account | Can Read (minimum) |
| `All Users` group | Can Read |
| `admins` group | Admin (if your user is admin) |

4. If missing, click **+ Add** → select your user or `All Users` → set **Can Read** → **Save**

> Notebooks in `/Shared/` are usually accessible to all workspace members. Notebooks in `/Users/<someone-else>/` are private by default — move the notebook to `/Shared/` if that is the case.

---

### Check 5 — Cluster Permission

ADF runs the notebook on a specific cluster. The PAT user must have **Can Attach To** or **Can Manage** on that cluster.

1. Databricks → **Compute** → click `dev-cluster`
2. Click the **Permissions** tab (or **⋮** → **Edit permissions**)
3. Confirm your user (or `All Users`) has **Can Attach To** or higher:

| Permission | What ADF needs |
|---|---|
| Can Attach To | Minimum — ADF can attach notebook runs to this cluster |
| Can Restart | Allows ADF to restart cluster if stopped |
| Can Manage | Full control — only needed for admin operations |

4. If missing, click **+ Add permission** → select your user or `All Users` → **Can Attach To** → **Save**

---

### Check 6 — ADF Managed Identity vs PAT Conflict

If the ADF Linked Service was created with **Managed Identity** (MSI) authentication instead of PAT, it needs a different setup. MSI requires your ADF instance's managed identity to be added as a user in the Databricks workspace.

To confirm which auth type your Linked Service uses:
1. ADF → Manage → Linked services → `ls_databricks_workspace` → **Edit**
2. Check the **Authentication type** field

| Auth type shown | What to do |
|---|---|
| `Access token` | Correct for PAT — check the token value is pasted correctly |
| `Managed Identity` | Add ADF's system-assigned identity as a Databricks workspace user |
| `Service Principal` | Requires SP to be added to workspace and have workspace-level role |

**Simplest fix: switch to Access token (PAT)** — it is the most straightforward method and works without any additional Azure AD configuration.

---

### Check 7 — Quick Test: Validate the PAT Directly

Test the PAT outside of ADF to confirm it works with the Databricks REST API.

Open **Azure Cloud Shell** (or any terminal with `curl`) and run:

```bash
curl -s \
  -H "Authorization: Bearer <YOUR_PAT_TOKEN>" \
  https://adb-<your-workspace-id>.azuredatabricks.net/api/2.0/clusters/list \
  | head -c 200
```

Expected good response:
```json
{"clusters":[{"cluster_id":"0612-...","cluster_name":"dev-cluster",...}]}
```

Expected bad response (access denied):
```json
{"error_code":"PERMISSION_DENIED","message":"Provided access token does not have required scopes: workspace"}
```

If the direct API call also fails → the token is invalid or the workspace URL is wrong. Regenerate the PAT (Step 1) and try again.

If the direct API call succeeds but ADF still fails → the issue is in the ADF Linked Service configuration (wrong URL or token pasted incorrectly).

---

## Troubleshooting

### Error: `access token does not have required scopes: workspace`

**Cause:** The token in the Linked Service is expired or incorrect.

**Fix:**
1. Databricks → User Settings → Access tokens → **Generate new token**
2. ADF → Manage → Linked services → `ls_databricks_workspace` → **Edit**
3. Replace the old token with the new one
4. **Test connection** → confirm success
5. **Publish All**

---

### Error: `Cluster does not exist or is not running`

**Cause:** `dev-cluster` was terminated (Databricks auto-terminates after inactivity).

**Fix Option A (quick):** Start the cluster manually before the ADF run.
1. Databricks → Compute → `dev-cluster` → **Start**
2. Wait ~2 minutes → re-run the ADF pipeline

**Fix Option B (permanent):** Switch the Linked Service to use a **Job cluster** instead.
1. ADF → Manage → Linked services → `ls_databricks_workspace` → Edit
2. Change **Select cluster** from `Existing interactive cluster` to `New job cluster`
3. Configure cluster size (e.g., `Standard_DS3_v2`, 1 worker) — ADF will provision a fresh cluster for each run
4. **Test connection** → **Create**

> Job cluster cold-start = 5-8 minutes added to each run.
> Interactive cluster (already warm) = no cold-start delay — preferred for frequent runs.

---

### Error: `Notebook not found at path /Shared/bronze_ingestion/02_bronze_blob_all_entities_v2`

**Cause:** Notebook not imported, or imported at a different path.

**Fix:**
1. Databricks → Workspace → Shared → confirm the notebook exists at that exact path
2. If missing, import from `day_6_blob_to_bronze_volume_migration/02_bronze_blob_all_entities_v2.ipynb`
3. In ADF Notebook activity → Settings tab → update the path to match

---

### Error: `RESOURCE_DOES_NOT_EXIST: Run xxx was not found`

**Cause:** ADF timed out waiting for the notebook run. The notebook may still be running in Databricks.

**Fix:**
1. Databricks → Workflows → **Job runs** → check for a recent run
2. If the run is still `In Progress`, wait for it to complete
3. Increase the ADF activity timeout: Notebook activity → **General** tab → **Timeout** → set to `02:00:00` (2 hours)

---

### Notebook Widget Error: `InputWidgetNotDefined: load_type`

**Cause:** The notebook cell that defines the widget (`dbutils.widgets.text(...)`) did not run before a later cell tried to read it. This happens when ADF passes parameters but the notebook had an error in Cell 2.

**Fix:**
1. Open the notebook in Databricks → run Cell 2 manually to confirm it creates the widget
2. Check that Cell 2 uses `dbutils.widgets.text("load_type", "incremental", ...)` with a default value
3. In ADF, confirm the Base parameter name is exactly `load_type` (case-sensitive, no spaces)

---

## Full Configuration Reference

### Linked Service Settings

| Setting | Value |
|---|---|
| Name | `ls_databricks_workspace` |
| Type | Azure Databricks |
| Workspace URL | `https://adb-<id>.azuredatabricks.net` |
| Authentication | Access token (PAT) |
| Cluster | Existing interactive — `dev-cluster` |
| Token expiry | 90 days (regenerate and update before expiry) |

### Notebook Activity Settings

| Setting | Value |
|---|---|
| Linked service | `ls_databricks_workspace` |
| Notebook path | `/Shared/bronze_ingestion/02_bronze_blob_all_entities_v2` |
| Base parameter: `load_type` | `incremental` (or `full` for backfill) |
| Timeout | `01:00:00` (1 hour — adjust if notebook takes longer) |
| Retry | `0` (no retry — notebook is idempotent, safe to re-run manually) |

---

## How the `load_type` Parameter Flows

```
ADF Pipeline
  └── Notebook Activity
        └── Base parameter: load_type = "incremental"
              └── Databricks notebook Cell 2
                    └── dbutils.widgets.text("load_type", "incremental", ...)
                          └── dbutils.widgets.get("load_type")  →  "incremental"
                                └── Used in Cell 3+ to decide full vs. incremental logic
```

The value you set in **ADF Base parameters** tab directly overrides the widget default inside the notebook.

---

## PAT Token Rotation (Every 90 Days)

Databricks PATs expire. Set a calendar reminder to rotate before expiry.

**When the token expires:**
1. ADF pipeline will fail with: `access token does not have required scopes: workspace`
2. Databricks → User Settings → Access tokens → **Generate new token**
3. ADF → Manage → Linked services → `ls_databricks_workspace` → **Edit**
4. Paste the new token → **Test connection** → **Create**
5. **Publish All**

> Consider storing the PAT in Azure Key Vault and referencing it in the Linked Service via `@Microsoft.KeyVault(...)` to make rotation easier — update Key Vault only, not the Linked Service.
