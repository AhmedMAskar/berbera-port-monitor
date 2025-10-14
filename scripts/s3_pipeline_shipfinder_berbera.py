# scripts/s3_pipeline_shipfinder_berbera.py
"""
Automated S3-only pipeline for VesselFinder Berbera page (Playwright, hardened):
- Uses realistic UA and masks webdriver.
- NO 'networkidle' wait (many sites never get idle).
- Gentle waits + scroll to trigger lazy content.
- Searches tables on main page and in iframes.
- Saves page.html & screenshot.png artifacts for debugging.
- Uploads CSV to:
    s3://<S3_BUCKET>/<S3_PREFIX>/latest/vf_snapshot.csv
    s3://<S3_BUCKET>/<S3_PREFIX>/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<UTC>.csv

Env:
  S3_BUCKET  (required)
  S3_PREFIX="berbera"
  AWS_REGION (e.g., us-east-1)
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
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

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

REAL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

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
# Scrape utilities
# -------------------------
def try_click_cookies(page):
    # attempt generic cookie banners; ignore failures
    selectors = [
        "button:has-text('Accept')",
        "button:has-text('I Agree')",
        "button:has-text('Agree')",
        "text=Accept all",
        "text=Accept All",
        "[id*='accept']",
        "[class*='accept']",
        "[aria-label*='accept']",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.is_visible():
                loc.click(timeout=1000)
                page.wait_for_timeout(500)
                print(f"🔘 Clicked consent: {sel}")
                return True
        except Exception:
            pass
    return False

def heading_to_status(h: str) -> Optional[str]:
    h = (h or "").lower().strip()
    for key, val in STATUS_FROM_HEADING.items():
        if key in h:
            return val
    return None

def parse_table_html(table_html: str, heading_text: str) -> pd.DataFrame:
    try:
        dfs = pd.read_html(table_html, flavor="bs4")
    except Exception:
        return pd.DataFrame(columns=APP_COLS)
    if not dfs:
        return pd.DataFrame(columns=APP_COLS)
    df = dfs[0]
    cols = [str(c).strip().lower() for c in df.columns]
    df.columns = cols

    out = pd.DataFrame()
    name_col = next((c for c in cols if "vessel" in c or "name" in c or "ship" in c), None)
    out["name"] = df[name_col].astype(str).str.strip() if name_col else None

    mmsi_col = next((c for c in cols if "mmsi" in c), None)
    out["mmsi"] = pd.to_numeric(df[mmsi_col], errors="coerce") if mmsi_col else None

    ship_col = next((c for c in cols if "type" in c), None)
    out["ship_type"] = df[ship_col].astype(str).str.title() if ship_col else "Unknown"

    last_port_col = next((c for c in cols if "destination" in c or "dest" in c or "from" in c or "origin" in c), None)
    out["last_port"] = df[last_port_col].astype(str) if last_port_col else None

    eta_col = next((c for c in cols if c == "eta" or "eta" in c), None)
    out["eta_to_berbera_utc"] = (
        pd.to_datetime(df[eta_col], errors="coerce", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if eta_col else None
    )

    spd_col = next((c for c in cols if "speed" in c or "kn" in c), None)
    out["speed_kn"] = (
        pd.to_numeric(df[spd_col].replace(r"[^\d\.]", "", regex=True), errors="coerce")
        if spd_col else None
    )

    null_mmsi = out["mmsi"].isna() if "mmsi" in out else pd.Series(dtype=bool)
    if null_mmsi.any():
        out.loc[null_mmsi, "mmsi"] = out.loc[null_mmsi, "name"].fillna("").map(synth_id)
    elif "mmsi" not in out:
        out["mmsi"] = out.get("name", "").fillna("").map(synth_id)

    out["distance_nm_to_berbera"] = None
    out["scraped_at_utc"] = now_utc_str()
    out["source"] = "vesselfinder"
    out["status"] = heading_to_status(heading_text) or "unknown"

    for c in APP_COLS:
        if c not in out.columns:
            out[c] = None
    return out[APP_COLS]

def collect_tables_from_context(ctx) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    tables = ctx.locator("table")
    try:
        count = tables.count()
    except Exception:
        count = 0
    for i in range(count):
        t = tables.nth(i)
        heading_text = ""
        try:
            heading_text = t.evaluate("""
                el => {
                  function text(el){ return (el && el.textContent||'').trim(); }
                  let cur = el;
                  for (let steps=0; steps<6 && cur; steps++) {
                    let sib = cur.previousElementSibling;
                    let checks = 0;
                    while (sib && checks < 8) {
                      if (['H1','H2','H3'].includes(sib.tagName)) return text(sib);
                      sib = sib.previousElementSibling; checks++;
                    }
                    cur = cur.parentElement;
                    if (cur) {
                      const h = cur.querySelector('h1,h2,h3');
                      if (h) return text(h);
                    }
                  }
                  return '';
                }
            """) or ""
        except Exception:
            heading_text = ""
        try:
            table_html = t.evaluate("el => el.outerHTML")
            if table_html:
                results.append((heading_text.lower(), table_html))
        except Exception:
            continue
    return results

def fetch_tables_with_headings(url: str, artifact_dir: str) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=REAL_UA,
            viewport={"width": 1440, "height": 900},
            java_script_enabled=True,
        )
        # mask webdriver
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = context.new_page()
        page.set_default_timeout(45000)

        print(f"🌐 Navigating: {url}")
        # Do NOT wait for "networkidle" (many sites never reach it)
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)

        # Give JS some time, try cookie click, then gentle scrolls
        try_click_cookies(page)
        for _ in range(6):  # scroll more to trigger lazy loads
            page.mouse.wheel(0, 1400)
            page.wait_for_timeout(700)

        # Soft wait for any table
        try:
            page.wait_for_selector("table", state="visible", timeout=8000)
        except PWTimeout:
            pass

        # Collect main-page tables
        main_tables = collect_tables_from_context(page)
        print(f"🔎 Tables on main page: {len(main_tables)}")
        results.extend(main_tables)

        # Also inspect iframes
        for fr in page.frames:
            if fr == page.main_frame:
                continue
            try:
                if fr.locator("table").count() > 0:
                    ftables = collect_tables_from_context(fr)
                    print(f"🔎 Tables in iframe: {len(ftables)}")
                    results.extend(ftables)
            except Exception:
                continue

        # Save artifacts to inspect what we saw
        try:
            os.makedirs(artifact_dir, exist_ok=True)
            with open(os.path.join(artifact_dir, "page.html"), "w", encoding="utf-8") as f:
                f.write(page.content())
            page.screenshot(path=os.path.join(artifact_dir, "screenshot.png"), full_page=True)
            print("🧩 Saved artifacts: scrape_artifacts/page.html & screenshot.png")
        except Exception as e:
            print(f"⚠️ Could not save artifacts: {e}")

        context.close()
        browser.close()
    return results

def normalize_concat(tables: List[Tuple[str, str]]) -> pd.DataFrame:
    if not tables:
        return pd.DataFrame(columns=APP_COLS)
    frames: List[pd.DataFrame] = []
    for heading, html in tables:
        frames.append(parse_table_html(html, heading))
    if not frames:
        return pd.DataFrame(columns=APP_COLS)
    df = pd.concat(frames, ignore_index=True)
    if not df.empty:
        df["name"] = df["name"].astype(str).str.strip()
        df["ship_type"] = df["ship_type"].astype(str).str.strip().str.title()
        df["status"] = df["status"].astype(str).str.strip().str.lower()
        df = df.dropna(subset=["name"]).drop_duplicates(subset=["mmsi","name","status"], keep="last")
    return df

# -------------------------
# Main
# -------------------------
def main():
    t0 = time.time()
    artifact_dir = os.path.join(os.getcwd(), "scrape_artifacts")
    tables = fetch_tables_with_headings(VF_URL, artifact_dir)
    print(f"✅ Found tables: {len(tables)}")

    df = normalize_concat(tables)
    print(f"✅ Normalized rows: {len(df)} | cols: {list(df.columns)}")

    latest_key, hist_key = write_outputs(df)
    print(f"📝 latest:  s3://{S3_BUCKET}/{latest_key}")
    print(f"🗄️ history: s3://{S3_BUCKET}/{hist_key}")
    print(f"⏱️ Done in {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
