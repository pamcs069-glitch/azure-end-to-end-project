"""
full_load_bronze.py
-------------------
Full load for all 17 VoltGrid API entities → Azure ADLS Bronze container.

Run locally ONCE before switching ADF to incremental mode.
ADF handles all future incremental loads automatically.

Output structure (mirrors what ADF v4 produces):
  bronze/<entity_name>/ingestion_date=<yyyy-MM-dd>/page_<N>.json

Usage:
  pip install requests azure-storage-blob python-dotenv tqdm
  python full_load_bronze.py
"""

import os
import json
import time
import datetime
import concurrent.futures
from dotenv import load_dotenv

import requests
from azure.storage.blob import BlobServiceClient
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

API_BASE        = "https://ev-project-navy-mu.vercel.app"
USERNAME        = os.getenv("VOLTGRID_USERNAME")
PASSWORD        = os.getenv("VOLTGRID_PASSWORD")
STORAGE_ACCOUNT = "evdatalakedev"
CONTAINER       = "bronze"
PAGE_SIZE       = 500
MAX_PAGES       = 1000          # hard cap per entity — prevents runaway loops
MAX_WORKERS     = 4             # parallel entities at a time — keep low to avoid API rate limits
RETRY_ATTEMPTS  = 3
RETRY_DELAY     = 5             # seconds between retries

TENANT_ID       = os.getenv("AZURE_TENANT_ID")
CLIENT_ID       = os.getenv("AZURE_PROJECT_CLIENT_ID")
CLIENT_SECRET   = os.getenv("AZURE_PROJECT_CLIENT_SECRET")

ENTITIES = [
    {"entity_name": "payments",           "api_path": "/api/db/payments/"},
    {"entity_name": "sessions",           "api_path": "/api/db/sessions/"},
    {"entity_name": "customers",          "api_path": "/api/db/customers/"},
    {"entity_name": "fleet",              "api_path": "/api/db/fleet/"},
    {"entity_name": "chargers",           "api_path": "/api/db/chargers/"},
    {"entity_name": "vehicles",           "api_path": "/api/db/vehicles/"},
    {"entity_name": "stations",           "api_path": "/api/db/stations/"},
    {"entity_name": "complaints",         "api_path": "/api/db/complaints/"},
    {"entity_name": "maintenance_events", "api_path": "/api/db/maintenance_events/"},
    {"entity_name": "energy_prices",      "api_path": "/api/db/energy_prices/"},
    {"entity_name": "tariffs",            "api_path": "/api/db/tariffs/"},
    {"entity_name": "charge_cards",       "api_path": "/api/db/charge_cards/"},
    {"entity_name": "employees",          "api_path": "/api/db/employees/"},
    {"entity_name": "partners",           "api_path": "/api/db/partners/"},
    {"entity_name": "cities",             "api_path": "/api/db/cities/"},
    {"entity_name": "states",             "api_path": "/api/db/states/"},
    {"entity_name": "weather",            "api_path": "/api/db/weather/"},
]

INGESTION_DATE = datetime.datetime.utcnow().strftime("%Y-%m-%d")
WATERMARK      = "1900-01-01T00:00:00Z"   # full load — fetch everything

# ── Auth helpers ──────────────────────────────────────────────────────────────

def get_api_token():
    """Login to VoltGrid API and return bearer token."""
    resp = requests.post(
        f"{API_BASE}/api/auth/login/",
        json={"username": USERNAME, "password": PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise ValueError(f"No token in login response: {resp.text}")
    print(f"[auth] Token obtained successfully")
    return token


def get_adls_client():
    """Return BlobServiceClient using Service Principal credentials."""
    from azure.identity import ClientSecretCredential
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
    account_url = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=credential)

# ── API fetch with retry ──────────────────────────────────────────────────────

def fetch_page(session, api_path, page, token):
    """Fetch one page from the API. Returns parsed JSON response."""
    url = (
        f"{API_BASE}{api_path}"
        f"?page={page}&page_size={PAGE_SIZE}&updated_after={WATERMARK}"
    )
    headers = {"Authorization": f"Token {token}"}

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == RETRY_ATTEMPTS:
                raise
            print(f"  [retry {attempt}/{RETRY_ATTEMPTS}] {e} — retrying in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)

# ── ADLS upload ───────────────────────────────────────────────────────────────

def upload_page(blob_client, entity_name, page, data):
    """Upload one page of results as JSON to Bronze ADLS."""
    blob_path = f"{entity_name}/ingestion_date={INGESTION_DATE}/page_{page}.json"
    content   = json.dumps(data, ensure_ascii=False)
    blob_client.get_blob_client(container=CONTAINER, blob=blob_path).upload_blob(
        content.encode("utf-8"),
        overwrite=True,
        content_settings=None,
    )

# ── Per-entity full load ──────────────────────────────────────────────────────

def load_entity(entity, token, blob_client):
    """Full load for one entity. Fetches all pages and uploads to Bronze."""
    name     = entity["entity_name"]
    api_path = entity["api_path"]

    session = requests.Session()

    # fetch page 1 to learn total_pages
    try:
        first = fetch_page(session, api_path, 1, token)
    except Exception as e:
        print(f"[{name}] FAILED to fetch page 1: {e}")
        return {"entity": name, "status": "failed", "pages_done": 0, "error": str(e)}

    total_pages  = first.get("pagination", {}).get("total_pages", 1)
    total_pages  = min(total_pages, MAX_PAGES)
    total_records = first.get("pagination", {}).get("total", "?")

    print(f"[{name}] {total_pages} pages | ~{total_records} records — starting upload")

    # upload page 1
    try:
        upload_page(blob_client, name, 1, first)
    except Exception as e:
        print(f"[{name}] FAILED to upload page 1: {e}")
        return {"entity": name, "status": "failed", "pages_done": 0, "error": str(e)}

    # fetch and upload remaining pages
    with tqdm(total=total_pages, desc=f"{name:25s}", unit="page", leave=True) as bar:
        bar.update(1)
        for page in range(2, total_pages + 1):
            try:
                data = fetch_page(session, api_path, page, token)
                upload_page(blob_client, name, page, data)
                bar.update(1)
            except Exception as e:
                print(f"\n[{name}] FAILED on page {page}: {e}")
                return {"entity": name, "status": "failed", "pages_done": page - 1, "error": str(e)}

    return {"entity": name, "status": "succeeded", "pages_done": total_pages, "error": None}

# ── Audit CSV update ──────────────────────────────────────────────────────────

def append_audit_rows(blob_client, results):
    """Append one row per entity to bronze/audit/pipeline_audit.csv."""
    audit_blob = blob_client.get_blob_client(container=CONTAINER, blob="audit/pipeline_audit.csv")

    # read existing content
    try:
        existing = audit_blob.download_blob().readall().decode("utf-8")
    except Exception:
        existing = "pipeline_name,entity_name,load_type,watermark_value,ingestion_date,total_pages,status,pipeline_run_id,run_timestamp\n"

    run_ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    run_id = f"local-full-{INGESTION_DATE}"

    new_rows = ""
    for r in results:
        new_rows += (
            f"full_load_bronze.py,{r['entity']},full,{WATERMARK},"
            f"{INGESTION_DATE},{r['pages_done']},{r['status']},{run_id},{run_ts}\n"
        )

    updated = existing.rstrip("\n") + "\n" + new_rows
    audit_blob.upload_blob(updated.encode("utf-8"), overwrite=True)
    print(f"\n[audit] pipeline_audit.csv updated with {len(results)} rows")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  VoltGrid Full Load — {INGESTION_DATE}")
    print(f"  Entities : {len(ENTITIES)}")
    print(f"  Max pages: {MAX_PAGES} per entity")
    print(f"  Workers  : {MAX_WORKERS} parallel entities")
    print("=" * 60)

    token       = get_api_token()
    blob_client = get_adls_client()

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(load_entity, entity, token, blob_client): entity["entity_name"]
            for entity in ENTITIES
        }
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                results.append(result)
                status = result["status"]
                pages  = result["pages_done"]
                print(f"[done] {name:25s} — {status} ({pages} pages)")
            except Exception as e:
                print(f"[error] {name}: {e}")
                results.append({"entity": name, "status": "failed", "pages_done": 0, "error": str(e)})

    append_audit_rows(blob_client, results)

    print("\n" + "=" * 60)
    succeeded = [r for r in results if r["status"] == "succeeded"]
    failed    = [r for r in results if r["status"] != "succeeded"]
    print(f"  Succeeded: {len(succeeded)}/{len(ENTITIES)}")
    if failed:
        print(f"  Failed   : {len(failed)}")
        for r in failed:
            print(f"    - {r['entity']}: {r['error']}")
    print("=" * 60)
    print("\nFull load complete. Switch ADF pl_bronze_api_master_v4 to incremental mode now.")


if __name__ == "__main__":
    main()
