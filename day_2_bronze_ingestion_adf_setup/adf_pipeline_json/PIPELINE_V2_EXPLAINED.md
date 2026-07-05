# pl_bronze_api_payments_v2 — Full & Incremental Pipeline Explained

---

## What this pipeline does

> Fetches credentials from Key Vault → logs in → runs full OR incremental load based on parameter → paginates through ALL pages → stores each page as a dated JSON file in Bronze layer.

---

## Full Load vs Incremental Load

### Why two modes?

Payment records get **updated** over time — a payment that was `pending` yesterday becomes `completed` today. So you cannot just fetch new records — you also need updated ones.

| Mode | When to use | What it fetches | `updated_after` filter |
|---|---|---|---|
| `full` | First run ever | ALL payments, all pages | None — gets everything since beginning of time |
| `incremental` | Every daily run after | Only payments created or updated since last run | `updated_after = last run's watermark` |

### How the VoltGrid API supports this

```
Full load:
  GET /api/db/payments/?page=1&page_size=100
  → returns ALL records

Incremental:
  GET /api/db/payments/?page=1&page_size=100&updated_after=2026-07-04T00:00:00Z
  → returns only records where updated_at > 2026-07-04T00:00:00Z
```

The `updated_at` field on each payment record changes whenever the payment status changes. So filtering by `updated_after` catches both new payments AND status updates on existing ones.

---

## Pipeline Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│                     pl_bronze_api_payments_v2                        │
│                                                                      │
│  INPUT PARAMETERS                                                    │
│  ┌──────────────────────┐   ┌────────────────────────────────┐       │
│  │  p_load_type         │   │  p_watermark                   │       │
│  │  "full" or           │   │  "2026-07-04T00:00:00Z"        │       │
│  │  "incremental"       │   │  (only used if incremental)    │       │
│  └──────────┬───────────┘   └───────────────┬────────────────┘       │
│             │                               │                        │
│             └───────────────┬───────────────┘                        │
│                             │                                        │
│                             ▼                                        │
│  ┌──────────────────────────────────┐                                │
│  │  act_get_username                │  Web Activity                  │
│  │  GET Key Vault → voltgrid-username│  Auth: Managed Identity       │
│  └─────────────────┬────────────────┘                                │
│                    │ on Success                                       │
│                    ▼                                                  │
│  ┌──────────────────────────────────┐                                │
│  │  act_get_password                │  Web Activity                  │
│  │  GET Key Vault → voltgrid-password│  Auth: Managed Identity       │
│  └─────────────────┬────────────────┘                                │
│                    │ on Success                                       │
│                    ▼                                                  │
│  ┌──────────────────────────────────┐                                │
│  │  act_api_login                   │  Web Activity                  │
│  │  POST /api/auth/login/           │  Returns: { token: "abc..." }  │
│  └─────────────────┬────────────────┘                                │
│                    │ on Success                                       │
│                    ▼                                                  │
│  ┌──────────────────────────────────┐                                │
│  │  act_set_token                   │  Set Variable                  │
│  │  v_token = output.token          │                                │
│  └─────────────────┬────────────────┘                                │
│                    │ on Success                                       │
│                    ▼                                                  │
│  ┌──────────────────────────────────┐                                │
│  │  act_set_ingestion_date          │  Set Variable                  │
│  │  v_ingestion_date = today's date │  e.g. "2026-07-04"             │
│  └─────────────────┬────────────────┘                                │
│                    │ on Success                                       │
│                    ▼                                                  │
│  ┌──────────────────────────────────┐                                │
│  │  act_set_watermark               │  Set Variable                  │
│  │                                  │                                │
│  │  if p_load_type == "full"        │                                │
│  │    v_watermark = 1900-01-01...   │  ← no filter, gets everything  │
│  │  else                            │                                │
│  │    v_watermark = p_watermark     │  ← yesterday's date passed in  │
│  └─────────────────┬────────────────┘                                │
│                    │ on Success                                       │
│                    ▼                                                  │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │  act_paginate  (Until Loop)                                  │    │
│  │  runs while: v_current_page <= v_total_pages                 │    │
│  │                                                              │    │
│  │  ┌────────────────────────────────────────────────────────┐  │    │
│  │  │  act_copy_payments_page          Copy Activity         │  │    │
│  │  │                                                        │  │    │
│  │  │  SOURCE: ds_voltgrid_payments_src_v2                   │  │    │
│  │  │    GET /api/db/payments/                               │  │    │
│  │  │        ?page={v_current_page}                          │  │    │
│  │  │        &page_size=100                                  │  │    │
│  │  │        &updated_after={v_watermark}  ← key filter      │  │    │
│  │  │    Authorization: Token {v_token}                      │  │    │
│  │  │                                                        │  │    │
│  │  │  SINK: ds_bronze_payments_sink_v2                      │  │    │
│  │  │    bronze/api/payments/raw/                            │  │    │
│  │  │    ingestion_date=2026-07-04/                          │  │    │
│  │  │    page_1.json                                         │  │    │
│  │  └───────────────────────┬────────────────────────────────┘  │    │
│  │                          │ on Success                        │    │
│  │                          ▼                                   │    │
│  │  ┌────────────────────────────────────────────────────────┐  │    │
│  │  │  act_set_temp_page                                     │  │    │
│  │  │  v_temp_page = v_current_page + 1                      │  │    │
│  │  └───────────────────────┬────────────────────────────────┘  │    │
│  │                          │ on Success                        │    │
│  │                          ▼                                   │    │
│  │  ┌────────────────────────────────────────────────────────┐  │    │
│  │  │  act_increment_page                                    │  │    │
│  │  │  v_current_page = v_temp_page                          │  │    │
│  │  └────────────────────────────────────────────────────────┘  │    │
│  │  (loop back to check condition)                               │    │
│  └──────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Why Two Variables to Increment (v_temp_page + v_current_page)?

ADF does not allow a variable to reference itself in a SetVariable expression:
```
v_current_page = add(v_current_page, 1)   ← NOT allowed — self-reference error
```

Workaround — use a temp variable:
```
Step 1: v_temp_page    = add(v_current_page, 1)   ← read old value, add 1
Step 2: v_current_page = v_temp_page               ← write result back
```

Two steps, no self-reference, same result.

---

## Watermark Logic

```
p_load_type = "full"
  → v_watermark = "1900-01-01T00:00:00Z"
  → API call: GET /api/db/payments/?page=1&page_size=100&updated_after=1900-01-01T00:00:00Z
  → every record in the system has updated_at > 1900 → returns everything

p_load_type = "incremental"
  → v_watermark = p_watermark  (e.g. "2026-07-04T00:00:00Z" — passed by the trigger)
  → API call: GET /api/db/payments/?page=1&page_size=100&updated_after=2026-07-04T00:00:00Z
  → only records updated on or after July 4 are returned
```

---

## Output Folder Structure in ADLS

```
evdatalakedev
└── bronze/
    └── api/
        └── payments/
            └── raw/
                ├── ingestion_date=2026-07-04/    ← full load, first run
                │   ├── page_1.json               ← records 1-100
                │   ├── page_2.json               ← records 101-200
                │   └── page_125.json             ← last page
                │
                ├── ingestion_date=2026-07-05/    ← incremental, day 2
                │   └── page_1.json               ← only records updated on July 5
                │
                └── ingestion_date=2026-07-06/    ← incremental, day 3
                    └── page_1.json
```

Each day's run lands in its own folder. No overwriting. Bronze is append-only.

---

## How to Trigger

### First run — Full load
| Parameter | Value |
|---|---|
| `p_load_type` | `full` |
| `p_watermark` | leave blank |

### Daily run — Incremental
| Parameter | Value |
|---|---|
| `p_load_type` | `incremental` |
| `p_watermark` | `2026-07-04T00:00:00Z` ← previous run's date at midnight UTC |

> **p_watermark format must be ISO 8601:** `YYYY-MM-DDTHH:MM:SSZ`
> For daily runs pass the start of the previous day: `2026-07-04T00:00:00Z`

---

## Paste Order in ADF Studio

1. `ds_voltgrid_payments_src_v2` — source dataset
2. `ds_bronze_payments_sink_v2` — sink dataset
3. `pl_bronze_api_payments_v2` — pipeline

Always create datasets before the pipeline — the pipeline JSON references them by name.

---

## Common Errors

| Error | Cause | Fix |
|---|---|---|
| `act_set_watermark` sets wrong value | `p_load_type` passed as `Full` (capital F) | ADF `equals()` is case-sensitive — always pass `full` in lowercase |
| Until loop runs only once | `v_total_pages` not updated from API response | The Copy Activity reads `total_pages` from pagination response — check the first page's JSON has `pagination.total_pages` |
| Incremental returns all records | `p_watermark` left blank on incremental run | Always pass the watermark date when `p_load_type = incremental` |
| Files overwrite previous run | Same `ingestion_date` used for two runs on same day | Expected — two runs on the same day share the same folder. Re-runs are safe because Databricks Silver uses MERGE. |
