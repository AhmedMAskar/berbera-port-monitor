# scripts/s3_pipeline_shipfinder_berbera.py
"""
Berbera Port Monitor — VesselFinder → S3 pipeline (detail-page MMSI/IMO enrichment, polite retries)
- Reads the port page tables (main + iframes).
- For each row, captures the vessel-name hyperlink → absolute detail_url.
- Visits detail pages (prioritize in_port, then incoming/expected/outgoing) and parses:
    * "IMO / MMSI" → IMO (7 digits), MMSI (9 digits)
    * destination, last port, ATD, course/speed, draught, flag, callsign
    * coordinates if exposed (for true tracks)
- Writes:
    s3://{S3_BUCKET}/{S3_PREFIX}/latest/vf_snapshot.csv
    s3://{S3_BUCKET}/{S3_PREFIX}/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<UTC>.csv
    s3://{S3_BUCKET}/{S3_PREFIX}/history/in_port/YYYY/MM/DD/%H%M/in_port_<UTC>.csv

Env:
  S3_BUCKET   (required)
  S3_PREFIX   (default 'berbera')
  AWS_REGION  (optional)
  VF_URL      (default https://www.vesselfinder.com/ports/SOBBO001)
  MAX_DETAIL_VESSELS (default 60)
  DETAIL_RETRIES (default 3)
  DETAIL_BACKOFF_BASE_MS (default 600)   # base backoff
  DETAIL_BACKOFF_MAX_MS (default 6000)   # cap backoff
  DETAIL_SLEEP_BETWEEN_MS (default 250)  # throttle between detail pages
"""

import os
import re
import zlib
import time
import random
import datetime as dt
from typing import List, Tuple, Optional, Dict

import pandas as pd
import boto3
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# Config / ENV
# =========================
def get_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip().strip('"').strip("'")

S3_BUCKET  = get_env("S3_BUCKET")
S3_PREFIX  = (get_env("S3_PREFIX") or "berbera").strip().strip("/")
AWS_REGION = get_env("AWS_REGION") or None
VF_URL     = get_env("VF_URL") or "https://www.vesselfinder.com/ports/SOBBO001"

MAX_DETAIL_VESSELS       = int(get_env("MAX_DETAIL_VESSELS", "60"))
DETAIL_RETRIES           = int(get_env("DETAIL_RETRIES", "3"))
DETAIL_BACKOFF_BASE_MS   = int(get_env("DETAIL_BACKOFF_BASE_MS", "600"))
DETAIL_BACKOFF_MAX_MS    = int(get_env("DETAIL_BACKOFF_MAX_MS", "6000"))
DETAIL_SLEEP_BETWEEN_MS  = int(get_env("DETAIL_SLEEP_BETWEEN_MS", "250"))

if not S3_BUCKET:
    raise SystemExit("❌ S3_BUCKET is required.")

# =========================
# Constants / UA
# =========================
STATUS_FROM_HEADING = {
    "arrivals": "incoming", "arrival": "incoming", "incoming": "incoming",
    "departures": "outgoing","departure": "outgoing","outgoing": "outgoing",
    "expected": "expected",
    "in port": "in_port","in-port": "in_port","inport": "in_port",
}

APP_COLS = [
    # identity
    "name","mmsi","imo","callsign",
    # type/status
    "ship_type","status","last_port","destination",
    # kinematics/physical
    "speed_kn","course_deg","heading_deg","draught_m",
    "gt","dwt","length_m","beam_m","built_year",
    # timing / provenance
    "eta_to_berbera_utc","atd_last_port_utc","scraped_at_utc","source",
    # TEU
    "teu_capacity_actual","teu_equiv",
    # enrichment
    "detail_url","last_port_detailed","nav_status","position_age_min","flag",
    "lat_deg","lon_deg",
    # internal helper (not used by app for IDs)
    "synth_id",
]

REAL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

# TEU estimation coefficients (match the app)
TEU_PER_TON = 1/12.0         # DWT → TEU_equiv for non-container cargo
K_LxB       = 0.50           # Container TEU ≈ k * L * B
INCLUDE_PASSENGER_AS_TEU = False

# =========================
# Helpers
# =========================
def s3() -> boto3.client:
    return boto3.client("s3", region_name=AWS_REGION)

def put_csv(bucket: str, key: str, csv_bytes: bytes):
    s3().put_object(
        Bucket=bucket, Key=key, Body=csv_bytes, ContentType="text/csv",
        CacheControl="no-cache, no-store, max-age=0, must-revalidate",
    )
    print(f"✅ Uploaded: s3://{bucket}/{key} (no-cache)")

def synth_id(name: str) -> int:
    """Stable internal id (for debug only). Not exported as MMSI."""
    return 0 if not name else abs(zlib.crc32(name.encode("utf-8")))

def now_utc_str() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_size_len_beam(text: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not text:
        return (None, None)
    nums = re.findall(r"(\d+(?:\.\d+)?)", str(text))
    if len(nums) >= 2:
        try:
            return float(nums[0]), float(nums[1])
        except Exception:
            return (None, None)
    return (None, None)

def num_clean(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce")

def coerce_eta(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce", utc=True)
    return s.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def teu_from_dwt(dwt: Optional[float]) -> float:
    return float(dwt) * TEU_PER_TON if dwt and dwt > 0 else 0.0

def teu_from_gt(gt: Optional[float]) -> float:
    return float(gt) / 10.0 if gt and gt > 0 else 0.0

def teu_from_lxb(length_m: Optional[float], beam_m: Optional[float]) -> float:
    if not length_m or not beam_m:
        return 0.0
    return max(50.0, min(K_LxB * float(length_m) * float(beam_m), 24000.0))

def hybrid_container_teu(stype: str, dwt=None, gt=None, length_m=None, beam_m=None, teu_actual=None) -> float:
    if "container" not in (stype or "").lower():
        return 0.0
    if teu_actual is not None:
        try:
            v = float(teu_actual)
            if v > 0: return v
        except Exception:
            pass
    cands = []
    if dwt: cands.append(teu_from_dwt(dwt))
    if gt:  cands.append(teu_from_gt(gt))
    if length_m and beam_m: cands.append(teu_from_lxb(length_m, beam_m))
    return float(sum(cands)/len(cands)) if cands else 0.0

def teu_equivalent_for_row(stype: str, dwt=None, gt=None, length_m=None, beam_m=None, teu_actual=None) -> float:
    st = (stype or "").lower()
    if "tug" in st or "sailing" in st: return 0.0
    if "container" in st: return round(hybrid_container_teu(st, dwt, gt, length_m, beam_m, teu_actual), 1)
    if "ro-ro" in st or "ro/ro" in st or "roro" in st: return round(teu_from_gt(gt), 1)
    if "passenger" in st: return round(teu_from_gt(gt), 1) if INCLUDE_PASSENGER_AS_TEU else 0.0
    if any(k in st for k in ["livestock","bulk","general cargo","cargo ship","tanker","oil","chemical"]):
        return round(teu_from_dwt(dwt), 1)
    v = teu_from_gt(gt) or teu_from_dwt(dwt)
    return round(v, 1) if v else 0.0

# =========================
# Table parsing (capture name hyperlink → detail_url)
# =========================
NAME_PATTERNS  = ["vessel","ship name","ship","name"]
TYPE_PATTERNS  = ["type","ship type"]
FROM_PATTERNS  = ["from","origin","last port","previous","prev port"]
TO_PATTERNS    = ["to","destination","dest","next port","port of call"]
ETA_PATTERNS   = ["eta","arrives","arrival","atd/eta"]
SPEED_PATTERNS = ["speed","kn","knots"]
MMSI_PATTERNS  = ["mmsi"]
GT_PATTERNS    = ["gt","gross"]
DWT_PATTERNS   = ["dwt","deadweight"]
SIZE_PATTERNS  = ["size","size (m)","length / beam","length/beam"]
BUILT_PATTERNS = ["built","year built"]

def pick_col(cols: List[str], patterns: List[str]) -> Optional[str]:
    for p in patterns:
        for c in cols:
            if p in c.lower(): return c
    return None

def heading_to_status(h: str) -> Optional[str]:
    h = (h or "").lower().strip()
    for k, v in STATUS_FROM_HEADING.items():
        if k in h: return v
    return None

def html_table_to_df(table_html: str) -> pd.DataFrame:
    soup = BeautifulSoup(table_html, "html.parser")
    table = soup.find("table")
    if table is None: return pd.DataFrame()
    # headers
    headers = []
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
    if not headers:
        first_tr = table.find("tr")
        if first_tr:
            headers = [td.get_text(strip=True) for td in first_tr.find_all(["th","td"])]
    # rows
    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all(["td","th"])
        if not tds: continue
        rows.append([td.get_text(strip=True) for td in tds])
    if rows and headers and [c.lower() for c in rows[0]] == [c.lower() for c in headers]:
        rows = rows[1:]
    width = max((len(headers), *(len(r) for r in rows)), default=0)
    if not headers or len(headers) < width:
        headers = headers + [f"col_{i}" for i in range(len(headers), width)]
    norm_rows = [r + [""]*(width - len(r)) for r in rows]
    df = pd.DataFrame(norm_rows, columns=headers).replace("", pd.NA).dropna(how="all")
    return df

def parse_table(table_html: str, heading_text: str) -> pd.DataFrame:
    df = html_table_to_df(table_html)
    if df.empty:
        return pd.DataFrame(columns=APP_COLS)

    cols = df.columns.tolist()
    name_col  = pick_col(cols, NAME_PATTERNS) or (cols[0] if cols else None)
    type_col  = pick_col(cols, TYPE_PATTERNS)
    from_col  = pick_col(cols, FROM_PATTERNS)
    to_col    = pick_col(cols, TO_PATTERNS)
    eta_col   = pick_col(cols, ETA_PATTERNS)
    speed_col = pick_col(cols, SPEED_PATTERNS)
    mmsi_col  = pick_col(cols, MMSI_PATTERNS)
    gt_col    = pick_col(cols, GT_PATTERNS)
    dwt_col   = pick_col(cols, DWT_PATTERNS)
    size_col  = pick_col(cols, SIZE_PATTERNS)
    built_col = pick_col(cols, BUILT_PATTERNS)

    out: Dict[str, pd.Series] = {}

    # --- Name and detail_url from hyperlink in the same row
    base = "https://www.vesselfinder.com"
    out["name"] = df[name_col].astype(str).str.strip() if name_col else pd.Series([None]*len(df))

    soup = BeautifulSoup(table_html, "html.parser")
    name_to_href: Dict[str, Optional[str]] = {}
    for tr in soup.find_all("tr"):
        # prefer the first <a> in the name cell if present
        a_tags = tr.find_all("a", href=True)
        if not a_tags: continue
        a = a_tags[0]
        tname = a.get_text(strip=True)
        href  = a["href"]
        if tname:
            name_to_href.setdefault(tname, href)

    def abs_url(h):
        if not h: return None
        return h if h.startswith("http") else base + h

    out["detail_url"] = out["name"].map(lambda n: abs_url(name_to_href.get(str(n).strip())))

    # MMSI column (table) — not trusted; will be overwritten by detail page
    if mmsi_col:
        out["mmsi"] = df[mmsi_col].astype(str).str.extract(r"(\d{6,10})", expand=False)
    else:
        out["mmsi"] = pd.NA

    # Ship type
    out["ship_type"] = (df[type_col].astype(str).str.strip().str.title() if type_col else pd.Series(["Unknown"]*len(df)))

    # Ports
    if from_col:
        out["last_port"] = df[from_col].astype(str).str.strip()
    elif to_col:
        out["last_port"] = df[to_col].astype(str).str.strip()
    else:
        out["last_port"] = pd.NA

    # ETA / Speed
    out["eta_to_berbera_utc"] = (coerce_eta(df[eta_col]) if eta_col else pd.NA)
    if speed_col:
        ser = df[speed_col].astype(str).str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False)
        out["speed_kn"] = pd.to_numeric(ser, errors="coerce")
    else:
        out["speed_kn"] = pd.NA

    # GT / DWT / Size / Built
    out["gt"]  = num_clean(df[gt_col])  if gt_col  else pd.NA
    out["dwt"] = num_clean(df[dwt_col]) if dwt_col else pd.NA

    length_m = pd.Series([pd.NA]*len(df)); beam_m = pd.Series([pd.NA]*len(df))
    if size_col:
        for i, txt in enumerate(df[size_col].astype(str)):
            L, B = parse_size_len_beam(txt)
            length_m.iloc[i] = L; beam_m.iloc[i] = B
    out["length_m"] = length_m; out["beam_m"] = beam_m
    out["built_year"] = num_clean(df[built_col]) if built_col else pd.NA

    # Defaults
    out["scraped_at_utc"] = now_utc_str()
    out["source"] = "vesselfinder"
    out["status"] = heading_to_status(heading_text) or "unknown"
    out["teu_capacity_actual"] = pd.NA

    out_df = pd.DataFrame(out)
    out_df["synth_id"] = out_df["name"].fillna("").map(synth_id)
    out_df["ship_type"] = out_df["ship_type"].astype(str).str.strip().str.title()
    out_df["status"]    = out_df["status"].astype(str).str.strip().lower()
    return out_df

# =========================
# Playwright capture (tables)
# =========================
def try_click_cookies(page):
    selectors = [
        "button:has-text('Accept')","button:has-text('I Agree')","button:has-text('Agree')",
        "text=Accept all","text=Accept All","[id*='accept']","[class*='accept']","[aria-label*='accept']",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.is_visible():
                loc.click(timeout=1000)
                page.wait_for_timeout(400)
                print(f"🔘 Clicked consent: {sel}")
                return True
        except Exception:
            pass
    return False

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

def fetch_tables_with_headings(url: str) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"],
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
            page.wait_for_timeout(650)

        try:
            page.wait_for_selector("table", state="visible", timeout=8000)
        except PWTimeout:
            pass

        # main page
        results.extend(collect_tables_from_context(page))

        # iframes
        for fr in page.frames:
            if fr == page.main_frame: continue
            try:
                if fr.locator("table").count() > 0:
                    results.extend(collect_tables_from_context(fr))
            except Exception:
                continue

        context.close()
        browser.close()
    print(f"🔎 Total tables captured: {len(results)}")
    return results

def normalize_concat(tables: List[Tuple[str, str]]) -> pd.DataFrame:
    if not tables:
        return pd.DataFrame(columns=APP_COLS)
    frames: List[pd.DataFrame] = []
    for heading, html in tables:
        frames.append(parse_table(html, heading))
    if not frames:
        return pd.DataFrame(columns=APP_COLS)
    df = pd.concat(frames, ignore_index=True)
    if not df.empty:
        df["name"] = df["name"].astype(str).str.strip()
        df["ship_type"] = df["ship_type"].astype(str).str.strip().str.title()
        df["status"] = df["status"].astype(str).str.strip().str.lower()
        df = df.dropna(subset=["name"]).drop_duplicates(subset=["name","status","detail_url"], keep="last")
    return df

# =========================
# Detail-page parsing
# =========================
def parse_detail_textual(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    def rex(pattern, group=1, flags=0):
        m = re.search(pattern, text, flags)
        return m.group(group).strip() if m else None

    # “IMO / MMSI”
    imo = rex(r"(?:IMO)\s*[/ ]\s*([0-9]{7})", flags=re.I) or rex(r"IMO\s*([0-9]{7})", flags=re.I)
    mmsi = rex(r"(?:MMSI)\s*[/ ]\s*([0-9]{9})", flags=re.I) or rex(r"MMSI\s*([0-9]{9})", flags=re.I)

    destination = rex(r"(?:Destination|DESTINATION)\s+([A-Za-z0-9 ,\-/()]+)")
    last_port_detailed = rex(r"(?:Last Port|Previous port|From|Origin)\s*([A-Za-z0-9 ,\-/()&]+)")
    atd_last_port_utc  = rex(r"ATD:\s*([A-Za-z0-9: ,\-]+UTC)")
    nav_status = rex(r"(?:Navigation Status|Status)\s*([A-Za-z \-/]+)")
    draught_m = rex(r"Current draught\s*([0-9]+(?:\.[0-9]+)?)\s*m")
    draught_m = float(draught_m) if draught_m else None

    # Course / Speed
    course_deg = rex(r"Course\s*/\s*Speed\s*([0-9]+(?:\.[0-9]+)?)\s*°", group=1)
    speed_kn   = rex(r"Course\s*/\s*Speed\s*[0-9]+(?:\.[0-9]+)?\s*°\s*/\s*([0-9]+(?:\.[0-9]+)?)\s*kn", group=1)
    course_deg = float(course_deg) if course_deg else None
    speed_kn   = float(speed_kn) if speed_kn else None

    # Position age (mins)
    position_age_min = None
    age_txt = rex(r"(?:Position received|Last report)\s*([A-Za-z0-9 :]+?ago)")
    if age_txt:
        m = re.search(r"(\d+)\s*(min|minute|minutes)", age_txt, re.I)
        if m: position_age_min = int(m.group(1))
        else:
            d = re.search(r"(\d+)\s*day", age_txt, re.I)
            h = re.search(r"(\d+)\s*hour", age_txt, re.I)
            if d: position_age_min = int(d.group(1))*24*60
            elif h: position_age_min = int(h.group(1))*60

    # Coordinates
    lat_deg = lon_deg = None
    cand = soup.find(attrs={"data-lat": True, "data-lon": True}) or soup.find(attrs={"data-latitude": True, "data-longitude": True})
    if cand:
        try:
            lat_deg = float(cand.get("data-lat") or cand.get("data-latitude"))
            lon_deg = float(cand.get("data-lon") or cand.get("data-longitude"))
        except Exception:
            pass
    if lat_deg is None or lon_deg is None:
        m = re.search(r'"lat(?:itude)?"\s*:\s*(-?\d+(?:\.\d+)?)\s*,\s*"lon(?:gitude)?"\s*:\s*(-?\d+(?:\.\d+)?)', html, re.I)
        if m:
            lat_deg = float(m.group(1)); lon_deg = float(m.group(2))
    if lat_deg is None or lon_deg is None:
        m = re.search(r"L\.(?:marker|circle)\(\s*\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]", html)
        if m:
            lat_deg = float(m.group(1)); lon_deg = float(m.group(2))

    callsign = rex(r"Callsign\s*([A-Za-z0-9\-]+)")
    flag     = rex(r"(?:AIS Flag|Flag)\s*([A-Za-z &]+)")

    return {
        "imo": imo, "mmsi": mmsi, "callsign": callsign,
        "destination": destination, "last_port_detailed": last_port_detailed, "atd_last_port_utc": atd_last_port_utc,
        "nav_status": nav_status, "draught_m": draught_m,
        "course_deg": course_deg, "speed_kn": speed_kn, "position_age_min": position_age_min,
        "lat_deg": lat_deg, "lon_deg": lon_deg, "flag": flag,
    }

def backoff_sleep(attempt: int):
    """Exponential backoff with jitter (in ms), capped."""
    base = DETAIL_BACKOFF_BASE_MS
    cap  = DETAIL_BACKOFF_MAX_MS
    # exponential growth
    delay = min(cap, base * (2 ** (attempt - 1)))
    # full jitter
    delay = random.randint(int(delay * 0.5), delay)
    time.sleep(delay / 1000.0)

def enrich_with_detail_pages(df: pd.DataFrame, max_n: int = 60, per_page_timeout_ms: int = 15000) -> pd.DataFrame:
    if df.empty or "detail_url" not in df.columns:
        return df

    # Priority order
    prio_order = {"in_port":0, "incoming":1, "expected":2, "outgoing":3}
    dfc = df.copy()
    dfc["__prio"] = dfc["status"].map(lambda s: prio_order.get(str(s).lower(), 99))

    # one row per vessel name (most stable anchor)
    targets = (dfc.dropna(subset=["detail_url","name"])
                 .sort_values(["__prio","name"])
                 .drop_duplicates(subset=["name"], keep="last")
                 .head(max_n))

    if targets.empty:
        return df

    out_rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"],
        )
        context = browser.new_context(user_agent=REAL_UA, viewport={"width": 1440, "height": 900})
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = context.new_page()
        page.set_default_timeout(per_page_timeout_ms)

        for _, r in targets.iterrows():
            url = r["detail_url"]
            name = r["name"]
            if not isinstance(url, str) or not url.startswith("http"):
                continue

            success = False
            for attempt in range(1, DETAIL_RETRIES + 1):
                try:
                    page.goto(url, wait_until="domcontentloaded")
                    page.wait_for_timeout(300)
                    html = page.content()
                    info = parse_detail_textual(html)
                    out_rows.append({**info, "name": name, "detail_url": url})
                    success = True
                    break
                except Exception as e:
                    print(f"⚠️ detail attempt {attempt}/{DETAIL_RETRIES}: {url} — {e}")
                    if attempt < DETAIL_RETRIES:
                        backoff_sleep(attempt)
                        continue
                # if fail all retries, we skip

            # polite throttle between vessels regardless of success/fail
            if DETAIL_SLEEP_BETWEEN_MS > 0:
                time.sleep(DETAIL_SLEEP_BETWEEN_MS / 1000.0)

        context.close()
        browser.close()

    if not out_rows:
        return df

    extra = pd.DataFrame(out_rows)
    # Keep only valid digit lengths for IDs
    if "mmsi" in extra.columns:
        extra.loc[~extra["mmsi"].astype(str).str.fullmatch(r"\d{9}", na=False), "mmsi"] = None
    if "imo" in extra.columns:
        extra.loc[~extra["imo"].astype(str).str.fullmatch(r"\d{7}", na=False), "imo"] = None

    merged = df.merge(extra, on=["name"], how="left", suffixes=("","_det"))

    # Prefer detail-page MMSI/IMO and other enrichments
    prefer_cols = [
        "mmsi","imo","callsign","destination","last_port_detailed","atd_last_port_utc",
        "course_deg","speed_kn","nav_status","position_age_min","draught_m","flag","lat_deg","lon_deg","detail_url"
    ]
    for col in prefer_cols:
        det = f"{col}_det"
        if det in merged.columns:
            merged[col] = merged[det].combine_first(merged[col])

    drop_cols = [c for c in merged.columns if c.endswith("_det")]
    merged = merged.drop(columns=drop_cols, errors="ignore")

    return merged

# =========================
# Outputs
# =========================
def compute_teu(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    vals = []
    for _, r in df.iterrows():
        vals.append(
            teu_equivalent_for_row(
                stype=r.get("ship_type"),
                dwt=r.get("dwt"),
                gt=r.get("gt"),
                length_m=r.get("length_m"),
                beam_m=r.get("beam_m"),
                teu_actual=r.get("teu_capacity_actual"),
            )
        )
    df = df.copy()
    df["teu_equiv"] = vals
    return df

def write_outputs(df: pd.DataFrame) -> Tuple[str, str, Optional[str]]:
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    latest_key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    hist_prefix = dt.datetime.utcnow().strftime(f"{S3_PREFIX}/history/csv/%Y/%m/%d/%H%M")
    hist_key = f"{hist_prefix}/vf_snapshot_{ts}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    put_csv(S3_BUCKET, latest_key, csv_bytes)
    put_csv(S3_BUCKET, hist_key, csv_bytes)

    # in-port (tug-free) dedicated history
    in_key = None
    dfx = df.copy()
    dfx["status"] = dfx["status"].astype(str).str.lower().str.strip()
    dfx["ship_type"] = dfx["ship_type"].astype(str).str.strip()
    mask_tug = dfx["ship_type"].astype(str).str.contains(r"\btug\b", case=False, na=False)
    df_in = dfx[(dfx["status"] == "in_port") & (~mask_tug)].copy()
    if not df_in.empty:
        in_prefix = dt.datetime.utcnow().strftime(f"{S3_PREFIX}/history/in_port/%Y/%m/%d/%H%M")
        in_key = f"{in_prefix}/in_port_{ts}.csv"
        put_csv(S3_BUCKET, in_key, df_in.to_csv(index=False).encode("utf-8"))

    return latest_key, hist_key, in_key

# =========================
# Main
# =========================
def main():
    t0 = time.time()

    # 1) Capture all port tables (main + iframes)
    tables = fetch_tables_with_headings(VF_URL)
    df = normalize_concat(tables)
    print(f"✅ Normalized rows: {len(df)} | cols: {list(df.columns)}")

    # 2) Enrich each vessel by visiting its detail page → MMSI/IMO etc. (with backoff)
    if not df.empty:
        df = enrich_with_detail_pages(df, max_n=MAX_DETAIL_VESSELS)

    # 3) Compute TEU equivalents
    df = compute_teu(df)

    # 4) Write S3 outputs
    latest_key, hist_key, in_key = write_outputs(df)
    print(f"📝 latest:  s3://{S3_BUCKET}/{latest_key}")
    print(f"🗄️ history: s3://{S3_BUCKET}/{hist_key}")
    if in_key:
        print(f"📦 in-port: s3://{S3_BUCKET}/{in_key}")

    print(f"⏱️ Done in {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
