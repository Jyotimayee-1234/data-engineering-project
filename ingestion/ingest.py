# Databricks notebook source
# Ingestion: syncs the BLS "pr" time-series directory and the population API into a Unity Catalog Volume.
#
# - Files are discovered dynamically from the BLS directory listing (no hardcoded filenames).
# - Re-runs are safe: HEAD metadata (ETag / Last-Modified / Content-Length) is compared with a manifest
#   in the Volume; unchanged files are skipped, changed or new files are downloaded, and files removed
#   at the source are deleted from the Volume and the manifest.
# - Downloads are written to a temp file and then moved into place.
# - A User-Agent with contact details is sent on every request, as the BLS data access policy requires.

# COMMAND ----------

import requests, re, json, os, shutil, time
from datetime import datetime, timezone

VOL      = "/Volumes/rearc/quest/raw"
BASE     = "https://download.bls.gov/pub/time.series/pr/"
BLS_DIR  = f"{VOL}/bls/pr"
MANIFEST = f"{VOL}/bls/_manifest.json"
POP_DIR  = f"{VOL}/population"
HEADERS  = {"User-Agent": "Jyotimayee Sahoo sahoo.jyotimayee13@gmail.com"}

def list_remote():
    r = requests.get(BASE, headers=HEADERS, timeout=30)
    r.raise_for_status()
    names = re.findall(r'HREF="/pub/time\.series/pr/([^"]+)"', r.text, flags=re.I)
    return sorted(set(names))

def load_manifest():
    if os.path.exists(MANIFEST):
        with open(MANIFEST) as f:
            return json.load(f)
    return {}

def sync_bls():
    os.makedirs(BLS_DIR, exist_ok=True)
    manifest, remote = load_manifest(), list_remote()
    stats = {"downloaded": [], "skipped": [], "removed": []}

    for name in remote:
        url = BASE + name
        h = requests.head(url, headers=HEADERS, timeout=30, allow_redirects=True)
        h.raise_for_status()
        sig = {k: h.headers.get(k) for k in ("ETag", "Last-Modified", "Content-Length")}
        if manifest.get(name, {}).get("sig") == sig:
            stats["skipped"].append(name); continue
        r = requests.get(url, headers=HEADERS, timeout=300)
        r.raise_for_status()
        tmp = f"{BLS_DIR}/.{name}.tmp"
        with open(tmp, "wb") as f: f.write(r.content)
        shutil.move(tmp, f"{BLS_DIR}/{name}")          # atomic-ish publish
        manifest[name] = {"sig": sig, "ingested_at": datetime.now(timezone.utc).isoformat()}
        stats["downloaded"].append(name)
        time.sleep(1)                                   # be polite, avoid rate limits

    for name in set(manifest) - set(remote):            # deleted at source
        p = f"{BLS_DIR}/{name}"
        if os.path.exists(p): os.remove(p)
        del manifest[name]; stats["removed"].append(name)

    with open(MANIFEST, "w") as f: json.dump(manifest, f, indent=2)
    return stats

def fetch_population():
    os.makedirs(POP_DIR, exist_ok=True)
    url = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
           "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation"
           "&locale=en&measures=Population")
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    payload = r.json()                       # fail early if not valid JSON
    assert payload.get("data"), "population response has no 'data' rows"
    with open(f"{POP_DIR}/population.json", "w") as f:
        json.dump(payload, f)
    print(f"population rows: {len(payload['data'])}")

# COMMAND ----------

# Sync BLS files. A second run with an unchanged source prints 'downloaded': [].
print(sync_bls())

# COMMAND ----------

# Fetch the population API response (overwritten on each run).
fetch_population()

# COMMAND ----------

# Optional check: inspect the landed population rows (expect 11 rows, 2013-2019 and 2021-2024).
p = json.load(open(f"{POP_DIR}/population.json"))
for row in p["data"]:
    print(row)
