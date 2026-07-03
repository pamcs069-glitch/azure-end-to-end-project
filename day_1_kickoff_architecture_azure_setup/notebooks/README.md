# Day 1 Notebooks

Four notebooks for Day 1. Import them into Databricks directly.

> **Storage connection — pick one approach:**
> - `00_mount_storage.ipynb` — legacy mount approach (requires **Dedicated** cluster mode)
> - `00b_connect_storage_no_mount.ipynb` — modern direct access, no mount (works in all cluster modes including Shared and Serverless)

## How to Import into Databricks

1. Databricks → left menu **Workspace**
2. Right-click any folder → **Import**
3. Select **File** → drag and drop the `.ipynb` file (or browse to it)
4. Click **Import**
5. The notebook opens — attach to `dev-cluster` from the top-right dropdown

Repeat for each `.ipynb` file below.

---

## Notebooks — Run in This Order

| # | File | Part in DAY1_AZURE_SETUP.md | What it does |
|---|---|---|---|
| 1a | `00_mount_storage.ipynb` | Part 7.2 | **(Legacy)** Mounts bronze/silver/gold/source using SP OAuth — requires Dedicated cluster |
| 1b | `00b_connect_storage_no_mount.ipynb` | Part 7.2 | **(Modern)** Direct ABFSS access — no mount, works in all cluster modes |
| 2 | `01_verify_api_auth.ipynb` | Part 7.3 | Tests VoltGrid API login, scans all 18 endpoints, runs noise check |
| 3 | `02_read_source_blob.ipynb` | Part 7.4 | Reads shared source blob data using SAS token |

Run **either 1a or 1b** — not both. Then run 2 and 3 in order.

---

## Before Running

- Cluster must be **started**
- For `00_mount_storage.ipynb` (1a): Access mode must be **Dedicated** — mount is blocked on Standard/Shared/Serverless
- For `00b_connect_storage_no_mount.ipynb` (1b): Any cluster mode works
- Secret scope `kv-ev-scope` must be created (Day 1 Part 6.5)
- All Key Vault secrets must exist (Day 1 Part 4 + 5)
- For `02_read_source_blob.ipynb` — SAS token secrets must be added to Key Vault (Day 1 Part 7.4.1)

## Re-running After Cluster Restart

- **If using 1a (mount):** Re-run `00_mount_storage.ipynb` after every cluster restart — mounts are lost when a cluster terminates.
- **If using 1b (no mount):** Re-run Cells 1–3 of `00b_connect_storage_no_mount.ipynb` — the Spark config is per-session and must be reset each time.
