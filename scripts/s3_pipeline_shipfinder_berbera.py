# scripts/s3_pipeline_vesselfinder.py
"""
Automated S3-only pipeline for VesselFinder Berbera page:
- Loads the page with Playwright (JS-rendered).
- Extracts tables + nearby headings, infers status category (in_port, incoming, outgoing, expected).
- Normalizes to the app schema.
- Uploads:
    s3://<S3_BUCKET>/<S3_PREFIX>/latest/vf_snapshot.csv
    s3://<S3_BUCKET>/<S3_PREFIX>/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<UTC>.csv

Env (set in GitHub Action step `env:`):
  S3_BUCKET            (required) e.g., berbera-port-monitor
  S3_PREFIX="berbera"  (recommended)
  AWS_REGION           e.g., us-east-1
  VF_URL="https://www.vesselfinder.com/ports/SOBBO001"
"""

import os
import io
import zlib
import time
import datetime as dt
from typing import List, Tuple, Optional

import pandas as pd
import boto3

# Playwright imports (installed via workflow)
from playwright.sync_api import sync_playwright

# -------------------------
# Config / Helpers
# -------------------------
def get_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip().strip('"').strip("'")

S3_BUCKET  = get_env("S3_BUCKET")
S3_PREFIX  = (get_env("S3_PREFIX") or "berbera").strip().strip("/")
AWS_REGION = get_env("AWS_REGION") or None
VF_URL     = get_env("VF_URL") or "https://www.vesselfinder.com/ports/SOBBO001"

if not S3_BUCKET:
    raise SystemExit("❌ S3_BUCKET is required.")

STATUS_FROM_HEADING = {
    "arrivals": "incoming",
    "arrival": "incoming",
    "incoming": "incoming",
    "departures": "outgoing",
    "departure": "outgoing",
    "outgoing": "outgoing",
    "expected": "expected",
    "in port": "in_port",
    "in-port": "in_port",
    "inport": "in_port",
}

APP_COLS = [
    "mmsi","name","ship_type","status","last_port",
    "distance_nm_to_berbera","eta_to_berbera_utc","speed_kn",
    "scraped_at_utc","source"
]

def synth_id(name: str) -> int:
    return 0 if not name else abs(zlib.crc32(name.encode("utf-8")))

def now_utc_str() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def s3() -> boto3.client:
    return boto3.client("s3", region_name=AWS_REGION)

def put_csv(bucket: str, key: str, csv_bytes: bytes):
    s3().put_object(
        Bucket=bucket, Key=key, Body=csv_bytes, ContentType="text/csv",
        CacheControl="no-cache, no-store, max-age=0, must-revalidate",
    )
    print(f"✅ Uploaded: s3://{bucket}/{key} (no-cache)")

def write_outputs(df: pd.DataFrame) -> Tuple[str, str]:
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    latest_key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    hist_prefix = dt.datetime.utcnow().strftime(f"{S3_PREFIX}/history/csv/%Y/%m/%d/%H%M")
    hist_key = f"{hist_prefix}/vf_snapshot_{ts}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    put_csv(S3_BUCKET, latest_key, csv_bytes)
    put_csv(S3_BUCKET, hist_key, csv_bytes)
    return latest_key, hist_key

# -------------------------
# Scrape + Parse
# -------------------------
def fetch_tables_with_headings(url: str) -> List[Tuple[str, str]]:
    """
    Returns list of (heading_text, table_html).
    We grab all tables and look backward for a nearby <h2>/<h3> or section title.
    """
    results: List[Tuple[str, str]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=120_000)
        # allow a short idle for dynamic content
        page.wait_for_timeout(1000)

        # Query all tables on the page
        tables = page.query_selector_all("table")
        for t in tables:
            # grab previous heading text if available
            heading_text = ""
            # try previous siblings up to a few steps for an h2/h3
            sibling = t
            for _ in range(5):
                sibling = sibling.evaluate_handle("el => el.previousElementSibling")  # JS handle
                if not sibling:
                    break
                try:
                    tag = sibling.evaluate("el => el.tagName.toLowerCase()")
                    if tag in ("h1", "h2", "h3"):
                        heading_text = sibling.evaluate("el => el.textContent || ''") or ""
                        heading_text = heading_text.strip()
                        break
                except Exception:
                    pass

            # fallback: look up in parent chain for a header element
            if not heading_text:
                try:
                    heading_text = t.evaluate("""
                        el => {
                          let cur = el.parentElement;
                          for (let i=0;i<4 && cur;i++){
                            const h = cur.querySelector('h1,h2,h3');
                            if (h) return h.textContent || '';
                            cur = cur.parentElement;
                          }
                          return '';
                        }
                    """) or ""
                    heading_text = heading_text.strip()
                except Exception:
                    pass

            table_html = t.evaluate("el => el.outerHTML")
            if table_html:
                results.append((heading_text.lower(), table_html))

        browser.close()
    return results

def heading_to_status(h: str) -> Optional[str]:
    h = (h or "").lower().strip()
    for key, val in STATUS_FROM_HEADING.items():
        if key in h:
            return val
    return None  # unknown; we’ll drop or mark "unknown"

def normalize_concat(tables: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    For each (heading, table_html):
      * parse with pandas.read_html
      * infer columns
      * assign 'status' from the heading
    """
    frames: List[pd.DataFrame] = []
    for heading, html in tables:
        try:
            dfs = pd.read_html(html, flavor="bs4")  # uses lxml/bs4
        except Exception:
            continue
        if not dfs:
            continue
        df = dfs[0]
        # unify columns
        cols = [str(c).strip().lower() for c in df.columns]
        df.columns = cols

        # try to extract name, mmsi, ship_type, destination/last_port, eta, speed
        out = pd.DataFrame()
        # name/vessel column (varies by site)
        name_col = next((c for c in cols if "vessel" in c or "name" in c or "ship" in c), None)
        if name_col:
            out["name"] = df[name_col].astype(str).str.strip()
        else:
            out["name"] = None

        # mmsi sometimes present; often not for these pages
        mmsi_col = next((c for c in cols if "mmsi" in c), None)
        if mmsi_col:
            out["mmsi"] = pd.to_numeric(df[mmsi_col], errors="coerce")
        else:
            out["mmsi"] = None

        # ship_type
        ship_col = next((c for c in cols if "type" in c), None)
        out["ship_type"] = df[ship_col].astype(str).str.title() if ship_col else "Unknown"

        # destination / last_port / origin
        last_port_col = next((c for c in cols if "destination" in c or "dest" in c or "from" in c or "origin" in c), None)
        out["last_port"] = df[last_port_col].astype(str) if last_port_col else None

        # eta
        eta_col = next((c for c in cols if c == "eta" or "eta" in c), None)
        if eta_col:
            out["eta_to_berbera_utc"] = pd.to_datetime(df[eta_col], errors="coerce", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            out["eta_to_berbera_utc"] = None

        # speed
        spd_col = next((c for c in cols if "speed" in c or "kn" in c), None)
        if spd_col:
            out["speed_kn"] = pd.to_numeric(df[spd_col].replace(r"[^\d\.]", "", regex=True), errors="coerce")
        else:
            out["speed_kn"] = None

        # fill MMSI if missing
        null_mmsi = out["mmsi"].isna()
        if null_mmsi.any():
            out.loc[null_mmsi, "mmsi"] = out.loc[null_mmsi, "name"].fillna("").map(synth_id)

        # defaults
        out["distance_nm_to_berbera"] = None
        out["scraped_at_utc"] = now_utc_str()
        out["source"] = "vesselfinder"

        # status: from heading keyword
        st = heading_to_status(heading) or "unknown"
        out["status"] = st

        # reorder
        for c in APP_COLS:
            if c not in out.columns:
                out[c] = None
        frames.append(out[APP_COLS])

    if not frames:
        return pd.DataFrame(columns=APP_COLS)
    all_df = pd.concat(frames, ignore_index=True)
    # Basic cleanup
    all_df["name"] = all_df["name"].astype(str).str.strip()
    all_df["ship_type"] = all_df["ship_type"].astype(str).str.strip().str.title()
    all_df["status"] = all_df["status"].astype(str).str.strip().str.lower()
    return all_df

# -------------------------
# Main
# -------------------------
def main():
    t0 = time.time()
    print(f"🌐 Loading VesselFinder page: {VF_URL}")
    tables = fetch_tables_with_headings(VF_URL)
    print(f"✅ Found tables: {len(tables)}")
    df = normalize_concat(tables)
    print(f"✅ Normalized rows: {len(df)} | cols: {list(df.columns)}")

    # Optional: drop empties / duplicates
    if not df.empty:
        df = df.dropna(subset=["name"]).drop_duplicates(subset=["mmsi","name","status"], keep="last")

    latest_key, hist_key = write_outputs(df)
    print(f"📝 latest:  s3://{S3_BUCKET}/{latest_key}")
    print(f"🗄️ history: s3://{S3_BUCKET}/{hist_key}")
    print(f"⏱️ Done in {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
