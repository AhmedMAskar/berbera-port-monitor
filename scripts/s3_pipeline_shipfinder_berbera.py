# scripts/s3_pipeline_shipfinder_berbera.py
"""
VesselFinder Berbera -> S3 (robust parsing)
- Headless Playwright (real UA, webdriver masked), no 'networkidle' wait.
- Scrolls & clicks generic cookie banners.
- Scrapes tables on main page + iframes.
- Parses tables via BeautifulSoup (robust to odd headers).
- Heuristics for column names; falls back to first column as vessel name.
- Saves artifacts (page.html, screenshot.png, each table as CSV) for debugging.
- Uploads to:
    s3://<S3_BUCKET>/<S3_PREFIX>/latest/vf_snapshot.csv
    s3://<S3_BUCKET>/<S3_PREFIX>/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<UTC>.csv
"""

import os
import io
import zlib
import csv
import time
import datetime as dt
from typing import List, Tuple, Optional, Dict

import pandas as pd
import boto3
from bs4 import BeautifulSoup
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

def html_table_to_df(table_html: str) -> pd.DataFrame:
    """Parse a <table> HTML to DataFrame via BeautifulSoup (robust and explicit)."""
    soup = BeautifulSoup(table_html, "html.parser")
    table = soup.find("table")
    if table is None:
        return pd.DataFrame()
    # headers
    headers = []
    header_row = table.find("thead")
    if header_row:
        ths = header_row.find_all("th")
        headers = [th.get_text(strip=True) for th in ths]
    if not headers:
        # try first row as header
        first_tr = table.find("tr")
        if first_tr:
            headers = [td.get_text(strip=True) for td in first_tr.find_all(["th","td"])]
    # rows
    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all(["td","th"])
        if not tds:
            continue
        rows.append([td.get_text(strip=True) for td in tds])
    # remove header row if duplicated
    if rows and headers and [c.lower() for c in rows[0]] == [c.lower() for c in headers]:
        rows = rows[1:]
    # ensure rectangular
    width = max((len(headers), *(len(r) for r in rows)), default=0)
    if not headers or len(headers) < width:
        headers = headers + [f"col_{i}" for i in range(len(headers), width)]
    norm_rows = [r + [""]*(width - len(r)) for r in rows]
    df = pd.DataFrame(norm_rows, columns=headers)
    # drop empty rows
    df = df.replace("", pd.NA).dropna(how="all")
    return df

# Flexible header matching
def pick_col(cols: List[str], patterns: List[str]) -> Optional[str]:
    lc = [c.lower().strip() for c in cols]
    for p in patterns:
        for c in cols:
            if p in c.lower():
                return c
    return None

NAME_PATTERNS  = ["vessel", "ship name", "ship", "name"]
TYPE_PATTERNS  = ["type", "ship type"]
FROM_PATTERNS  = ["from", "origin", "last port", "previous", "prev port"]
TO_PATTERNS    = ["to", "destination", "dest", "next port", "port of call"]
ETA_PATTERNS   = ["eta", "arrives", "arrival", "atd/eta"]
SPEED_PATTERNS = ["speed", "kn", "knots"]

def coerce_eta(series: pd.Series) -> pd.Series:
    # accept many human strings; keep best-effort UTC strings
    s = pd.to_datetime(series, errors="coerce", utc=True)
    return s.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_table(table_html: str, heading_text: str, save_csv_to: Optional[str] = None) -> pd.DataFrame:
    df = html_table_to_df(table_html)
    if df.empty:
        return pd.DataFrame(columns=APP_COLS)

    # Save raw table CSV (artifact) if path provided
    if save_csv_to:
        try:
            df.to_csv(save_csv_to, index=False)
        except Exception:
            pass

    cols = df.columns.tolist()
    # Identify columns
    name_col  = pick_col(cols, NAME_PATTERNS) or (cols[0] if cols else None)
    type_col  = pick_col(cols, TYPE_PATTERNS)
    from_col  = pick_col(cols, FROM_PATTERNS)
    to_col    = pick_col(cols, TO_PATTERNS)
    eta_col   = pick_col(cols, ETA_PATTERNS)
    speed_col = pick_col(cols, SPEED_PATTERNS)
    mmsi_col  = pick_col(cols, ["mmsi"])

    out: Dict[str, pd.Series] = {}

    # Name (fallback to first column)
    out["name"] = df[name_col].astype(str).str.strip() if name_col else pd.Series([None]*len(df))

    # MMSI (optional)
    if mmsi_col:
        out["mmsi"] = pd.to_numeric(df[mmsi_col].str.replace(r"[^\d]", "", regex=True), errors="coerce")
    else:
        out["mmsi"] = pd.NA

    # Ship type
    if type_col:
        out["ship_type"] = df[type_col].astype(str).str.strip().str.title()
    else:
        out["ship_type"] = "Unknown"

    # last_port: prefer "from/origin", otherwise "to/destination"
    if from_col:
        out["last_port"] = df[from_col].astype(str).str.strip()
    elif to_col:
        out["last_port"] = df[to_col].astype(str).str.strip()
    else:
        out["last_port"] = pd.NA

    # ETA
    if eta_col:
        out["eta_to_berbera_utc"] = coerce_eta(df[eta_col])
    else:
        out["eta_to_berbera_utc"] = pd.NA

    # speed
    if speed_col:
        ser = df[speed_col].astype(str).str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False)
        out["speed_kn"] = pd.to_numeric(ser, errors="coerce")
    else:
        out["speed_kn"] = pd.NA

    # defaults
    out["distance_nm_to_berbera"] = pd.NA
    out["scraped_at_utc"] = now_utc_str()
    out["source"] = "vesselfinder"
    out["status"] = heading_to_status(heading_text) or "unknown"

    out_df = pd.DataFrame(out)

    # MMSI synth if missing
    if out_df["mmsi"].isna().any():
        mask = out_df["mmsi"].isna()
        out_df.loc[mask, "mmsi"] = out_df.loc[mask, "name"].fillna("").map(synth_id)

    # reorder & clean
    for c in APP_COLS:
        if c not in out_df.columns:
            out_df[c] = pd.NA
    out_df = out_df[APP_COLS]
    out_df = out_df.dropna(subset=["name"]).replace({pd.NA: None})
    return out_df

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
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = context.new_page()
        page.set_default_timeout(45000)

        print(f"🌐 Navigating: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)

        try_click_cookies(page)
        for _ in range(6):
            page.mouse.wheel(0, 1400)
            page.wait_for_timeout(700)

        try:
            page.wait_for_selector("table", state="visible", timeout=8000)
        except PWTimeout:
            pass

        # main page
        main_tables = collect_tables_from_context(page)
        print(f"🔎 Tables on main page: {len(main_tables)}")
        results.extend(main_tables)

        # iframes too
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

        # artifacts
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

def normalize_concat(tables: List[Tuple[str, str]], artifact_dir: str) -> pd.DataFrame:
    if not tables:
        return pd.DataFrame(columns=APP_COLS)
    frames: List[pd.DataFrame] = []
    for idx, (heading, html) in enumerate(tables, start=1):
        # also save each table as CSV artifact for visibility
        table_csv = os.path.join(artifact_dir, f"table_{idx:02d}.csv")
        frames.append(parse_table(html, heading, save_csv_to=table_csv))
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

    df = normalize_concat(tables, artifact_dir)
    print(f"✅ Normalized rows: {len(df)} | cols: {list(df.columns)}")

    latest_key, hist_key = write_outputs(df)
    print(f"📝 latest:  s3://{S3_BUCKET}/{latest_key}")
    print(f"🗄️ history: s3://{S3_BUCKET}/{hist_key}")
    print(f"⏱️ Done in {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
