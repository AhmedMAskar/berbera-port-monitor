# scripts/s3_pipeline_shipfinder_berbera.py
"""
Berbera Port Monitor — VesselFinder → S3 pipeline (with detail-page enrichment)
- Headless Playwright (chromium) w/ webdriver masking.
- Scrapes main page + tables (and iframes).
- Builds rows: name, MMSI (or synthetic), type, status, last_port, ETA, speed, GT, DWT, LxB, Built
- Computes TEU/TEU-equivalent by ship type.
- Follows up to MAX_DETAIL_VESSELS detail pages to enrich:
    destination, last_port_detailed (+ ATD), course/heading, direction_cardinal,
    nav status, position age, draught, flag/IMO/callsign.
- Writes:
    s3://{S3_BUCKET}/{S3_PREFIX}/latest/vf_snapshot.csv
    s3://{S3_BUCKET}/{S3_PREFIX}/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<UTC>.csv
    s3://{S3_BUCKET}/{S3_PREFIX}/history/in_port/YYYY/MM/DD/HHmm/in_port_<UTC>.csv
"""

import os
import re
import zlib
import time
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
MAX_DETAIL_VESSELS = int(get_env("MAX_DETAIL_VESSELS", "10"))  # polite cap per run

if not S3_BUCKET:
    raise SystemExit("❌ S3_BUCKET is required.")

# =========================
# Status map
# =========================
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

# =========================
# Columns & UA
# =========================
APP_COLS = [
    "mmsi","name","ship_type","status","last_port",
    "distance_nm_to_berbera","eta_to_berbera_utc","speed_kn",
    "gt","dwt","length_m","beam_m","built_year",
    "teu_capacity_actual","teu_equiv",
    "scraped_at_utc","source",
    # detail-page enrichment
    "detail_url","destination","last_port_detailed","atd_last_port_utc",
    "course_deg","heading_deg","nav_status","direction_cardinal",
    "position_age_min","draught_m","flag","imo","callsign"
]

REAL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

# =========================
# TEU estimation coeffs
# =========================
TEU_PER_TON = 1/12.0
K_LxB       = 0.50
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
    return 0 if not name else abs(zlib.crc32(name.encode("utf-8")))

def now_utc_str() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def is_tug_series(ser: pd.Series) -> pd.Series:
    return ser.astype(str).str.contains(r"\btug\b", case=False, na=False)

def parse_size_len_beam(text: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not text:
        return (None, None)
    nums = re.findall(r"(\d+(?:\.\d+)?)", str(text))
    if len(nums) >= 2:
        try:
            return float(nums[0]), float(nums[1])
        except:
            return (None, None)
    return (None, None)

def teu_from_dwt(dwt: Optional[float]) -> float:
    return float(dwt) * TEU_PER_TON if dwt and dwt > 0 else 0.0

def teu_from_gt(gt: Optional[float]) -> float:
    return float(gt) / 10.0 if gt and gt > 0 else 0.0

def teu_from_lxb(length_m: Optional[float], beam_m: Optional[float]) -> float:
    if not length_m or not beam_m:
        return 0.0
    return max(50.0, min(K_LxB * float(length_m) * float(beam_m), 24000.0))

def hybrid_container_teu(ship_type: str, dwt=None, gt=None, length_m=None, beam_m=None, teu_actual=None) -> float:
    if "container" not in (ship_type or "").lower():
        return 0.0
    if teu_actual is not None:
        try:
            val = float(teu_actual)
            if val > 0:
                return val
        except:
            pass
    candidates = []
    if dwt: candidates.append(teu_from_dwt(dwt))
    if gt:  candidates.append(teu_from_gt(gt))
    if length_m and beam_m: candidates.append(teu_from_lxb(length_m, beam_m))
    return float(sum(candidates)/len(candidates)) if candidates else 0.0

def teu_equivalent_for_row(stype: str, dwt=None, gt=None, length_m=None, beam_m=None, lane_meters=None, teu_actual=None) -> float:
    stype_l = (stype or "").lower()
    if "tug" in stype_l or "sailing" in stype_l:
        return 0.0
    if "container" in stype_l:
        return round(hybrid_container_teu(stype_l, dwt=dwt, gt=gt, length_m=length_m, beam_m=beam_m, teu_actual=teu_actual), 1)
    if "ro-ro" in stype_l or "ro/ro" in stype_l or "roro" in stype_l:
        return round(teu_from_gt(gt), 1)
    if "passenger" in stype_l:
        return round(teu_from_gt(gt), 1) if INCLUDE_PASSENGER_AS_TEU else 0.0
    if any(k in stype_l for k in ["livestock", "bulk", "general cargo", "cargo ship", "tanker", "oil", "chemical"]):
        return round(teu_from_dwt(dwt), 1)
    val = teu_from_gt(gt) or teu_from_dwt(dwt)
    return round(val, 1) if val else 0.0

# =========================
# Table parsing
# =========================
NAME_PATTERNS  = ["vessel", "ship name", "ship", "name"]
TYPE_PATTERNS  = ["type", "ship type"]
FROM_PATTERNS  = ["from", "origin", "last port", "previous", "prev port"]
TO_PATTERNS    = ["to", "destination", "dest", "next port", "port of call"]
ETA_PATTERNS   = ["eta", "arrives", "arrival", "atd/eta"]
SPEED_PATTERNS = ["speed", "kn", "knots"]
MMSI_PATTERNS  = ["mmsi"]
GT_PATTERNS    = ["gt", "gross"]
DWT_PATTERNS   = ["dwt", "deadweight"]
SIZE_PATTERNS  = ["size", "size (m)", "length / beam", "length/beam"]
BUILT_PATTERNS = ["built", "year built"]

KNOWN_TYPE_PHRASES = [
    "General Cargo Ship",
    "Container Ship",
    "Bulk Carrier",
    "Cargo ship",
    "Oil Products Tanker",
    "Chemical/Oil Products Tanker",
    "Sailing vessel",
    "Livestock Carrier",
    "Tug",
    "Ro-Ro/Passenger Ship",
    "Passenger Ship",
]

def pick_col(cols: List[str], patterns: List[str]) -> Optional[str]:
    for p in patterns:
        for c in cols:
            if p in c.lower():
                return c
    return None

def heading_to_status(h: str) -> Optional[str]:
    h = (h or "").lower().strip()
    for key, val in STATUS_FROM_HEADING.items():
        if key in h:
            return val
    return None

def html_table_to_df(table_html: str) -> pd.DataFrame:
    soup = BeautifulSoup(table_html, "html.parser")
    table = soup.find("table")
    if table is None:
        return pd.DataFrame()

    headers = []
    thead = table.find("thead")
    if thead:
        ths = thead.find_all("th")
        headers = [th.get_text(strip=True) for th in ths]
    if not headers:
        first_tr = table.find("tr")
        if first_tr:
            headers = [td.get_text(strip=True) for td in first_tr.find_all(["th","td"])]

    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all(["td","th"])
        if not tds:
            continue
        rows.append([td.get_text(strip=True) for td in tds])

    if rows and headers and [c.lower() for c in rows[0]] == [c.lower() for c in headers]:
        rows = rows[1:]

    width = max((len(headers), *(len(r) for r in rows)), default=0)
    if not headers or len(headers) < width:
        headers = headers + [f"col_{i}" for i in range(len(headers), width)]
    norm_rows = [r + [""]*(width - len(r)) for r in rows]

    df = pd.DataFrame(norm_rows, columns=headers)
    df = df.replace("", pd.NA).dropna(how="all")
    return df

def split_name_and_type(raw: str) -> Tuple[str, Optional[str]]:
    if not raw:
        return "", None
    txt = str(raw).strip()
    for phrase in sorted(KNOWN_TYPE_PHRASES, key=len, reverse=True):
        if phrase.replace(" ", "").lower() in txt.replace(" ", "").lower():
            low = txt.lower()
            idx = low.rfind(phrase.lower())
            if idx != -1:
                name_part = txt[:idx].strip(" -–—·")
                return (name_part.strip(), phrase)
    return (txt, None)

def coerce_eta(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce", utc=True)
    return s.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_table(table_html: str, heading_text: str, save_csv_to: Optional[str] = None) -> pd.DataFrame:
    df = html_table_to_df(table_html)
    if df.empty:
        return pd.DataFrame(columns=APP_COLS)

    if save_csv_to:
        try:
            df.to_csv(save_csv_to, index=False)
        except Exception:
            pass

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

    # Name + detail_url
    if name_col:
        out["name"] = df[name_col].astype(str).str.strip()
        # recover anchor href by re-parsing raw HTML cells
        soup = BeautifulSoup(table_html, "html.parser")
        link_map = []
        body_rows = soup.find_all("tr")
        try:
            name_idx = list(df.columns).index(name_col)
        except ValueError:
            name_idx = 0
        for tr in body_rows:
            tds = tr.find_all(["td","th"])
            if not tds:
                continue
            href = None
            if name_idx < len(tds):
                a = tds[name_idx].find("a", href=True)
                if a and a.get("href"):
                    href = a["href"]
            link_map.append(href)
        if len(link_map) >= len(df)+1:
            link_map = link_map[-len(df):]
        out["detail_url"] = pd.Series(link_map[:len(df)])
    else:
        out["name"] = pd.Series([None]*len(df))
        out["detail_url"] = pd.Series([None]*len(df))

    # MMSI
    if mmsi_col:
        out["mmsi"] = pd.to_numeric(df[mmsi_col].astype(str).str.replace(r"[^\d]", "", regex=True), errors="coerce")
    else:
        out["mmsi"] = pd.NA

    # Ship type
    if type_col:
        out["ship_type"] = df[type_col].astype(str).str.strip().str.title()
    else:
        out["ship_type"] = "Unknown"

    # Ports
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

    # Speed
    if speed_col:
        ser = df[speed_col].astype(str).str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False)
        out["speed_kn"] = pd.to_numeric(ser, errors="coerce")
    else:
        out["speed_kn"] = pd.NA

    # GT / DWT
    def _num(series):
        return pd.to_numeric(series.astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce")
    out["gt"]  = _num(df[gt_col])  if gt_col  else pd.NA
    out["dwt"] = _num(df[dwt_col]) if dwt_col else pd.NA

    # Size (Length / Beam)
    length_m = pd.Series([pd.NA]*len(df))
    beam_m   = pd.Series([pd.NA]*len(df))
    if size_col:
        for i, txt in enumerate(df[size_col].astype(str)):
            L, B = parse_size_len_beam(txt)
            length_m.iloc[i] = L
            beam_m.iloc[i]   = B
    out["length_m"] = length_m
    out["beam_m"]   = beam_m

    # Built
    out["built_year"] = _num(df[built_col]) if built_col else pd.NA

    # Defaults / housekeeping
    out["distance_nm_to_berbera"] = pd.NA
    out["scraped_at_utc"] = now_utc_str()
    out["source"] = "vesselfinder"
    out["status"] = heading_to_status(heading_text) or "unknown"
    out["teu_capacity_actual"] = pd.NA

    out_df = pd.DataFrame(out)

    # Normalize detail_url to absolute
    base = "https://www.vesselfinder.com"
    if "detail_url" in out_df.columns:
        out_df["detail_url"] = out_df["detail_url"].astype(str)
        mask_rel = out_df["detail_url"].str.startswith("/")
        out_df.loc[mask_rel, "detail_url"] = base + out_df.loc[mask_rel, "detail_url"]

    # If ship_type unknown but embedded in name, split it
    if "ship_type" in out_df.columns:
        mask_unknown = out_df["ship_type"].isna() | (out_df["ship_type"] == "") | (out_df["ship_type"] == "Unknown")
        if mask_unknown.any():
            new_names, inferred_types = [], []
            for n in out_df.loc[mask_unknown, "name"].astype(str):
                nn, it = split_name_and_type(n)
                new_names.append(nn)
                inferred_types.append(it if it else "Unknown")
            out_df.loc[mask_unknown, "name"] = new_names
            out_df.loc[mask_unknown, "ship_type"] = inferred_types

    # MMSI synth if missing
    if out_df["mmsi"].isna().any():
        mask = out_df["mmsi"].isna()
        out_df.loc[mask, "mmsi"] = out_df.loc[mask, "name"].fillna("").map(synth_id)

    # TEU / TEU-equivalent
    teu_vals = []
    for _, r in out_df.iterrows():
        teu_vals.append(
            teu_equivalent_for_row(
                stype=r.get("ship_type"),
                dwt=r.get("dwt"),
                gt=r.get("gt"),
                length_m=r.get("length_m"),
                beam_m=r.get("beam_m"),
                lane_meters=None,
                teu_actual=r.get("teu_capacity_actual"),
            )
        )
    out_df["teu_equiv"] = teu_vals

    # reorder & clean
    for c in APP_COLS:
        if c not in out_df.columns:
            out_df[c] = pd.NA
    out_df = out_df[APP_COLS]
    out_df = out_df.dropna(subset=["name"]).replace({pd.NA: None})
    out_df["ship_type"] = out_df["ship_type"].astype(str).str.strip().str.title()
    out_df["status"] = out_df["status"].astype(str).str.strip().str.lower()
    return out_df

# =========================
# Playwright: capture tables and enrichment pages
# =========================
def try_click_cookies(page):
    selectors = [
        "button:has-text('Accept')",
        "button:has-text('I Agree')",
        "button:has-text('Agree')",
        "text=Accept all",
        "text=Accept All",
        "[id*='accept']","[class*='accept']","[aria-label*='accept']",
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
            page.wait_for_timeout(700)

        try:
            page.wait_for_selector("table", state="visible", timeout=8000)
        except PWTimeout:
            pass

        main_tables = collect_tables_from_context(page)
        print(f"🔎 Tables on main page: {len(main_tables)}")
        results.extend(main_tables)

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
            print("🧩 Saved artifacts in scrape_artifacts/")
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

# =========================
# Detail-page enrichment
# =========================
def cardinal_from_degrees(deg: Optional[float]) -> Optional[str]:
    if deg is None:
        return None
    dirs = ["N","NE","E","SE","S","SW","W","NW"]
    idx = int(((deg % 360) + 22.5) // 45) % 8
    return dirs[idx]

def parse_detail_textual(html: str) -> Dict[str, Optional[str]]:
    """
    Parse the vessel detail page using robust text/regex based on typical structure:
    - Destination / ETA
    - Course / Speed
    - Current draught
    - Navigation Status
    - Position received ... ago
    - IMO / MMSI
    - Callsign
    - AIS Flag
    - Length / Beam
    - Last Port ... ATD: ... UTC
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    def rex(pattern, group=1, flags=0):
        m = re.search(pattern, text, flags)
        return m.group(group).strip() if m else None

    destination = rex(r"(?:Destination|DESTINATION)\s+([A-Za-z0-9 ,\-/()]+)")

    course_deg = rex(r"Course\s*/\s*Speed\s*([0-9]+(?:\.[0-9]+)?)\s*°", group=1)
    speed_kn   = rex(r"Course\s*/\s*Speed\s*[0-9]+(?:\.[0-9]+)?\s*°\s*/\s*([0-9]+(?:\.[0-9]+)?)\s*kn", group=1)
    course_deg = float(course_deg) if course_deg else None
    speed_kn   = float(speed_kn) if speed_kn else None

    draught_m = rex(r"Current draught\s*([0-9]+(?:\.[0-9]+)?)\s*m")
    draught_m = float(draught_m) if draught_m else None

    nav_status = rex(r"(?:Navigation Status|Status)\s*([A-Za-z \-/]+)")

    position_age_min = None
    age_txt = rex(r"(?:Position received|Last report)\s*([A-Za-z0-9 :]+?ago)")
    if age_txt:
        m = re.search(r"(\d+)\s*(min|minute|minutes)", age_txt, re.I)
        if m: position_age_min = int(m.group(1))
        else:
            d = re.search(r"(\d+)\s*day", age_txt, re.I)
            h = re.search(r"(\d+)\s*hour", age_txt, re.I)
            if d: position_age_min = int(d.group(1)) * 24 * 60
            elif h: position_age_min = int(h.group(1)) * 60

    last_port_detailed = rex(r"(?:Last Port|Previous port|From|Origin)\s*([A-Za-z0-9 ,\-/()&]+)")
    atd_last_port_utc  = rex(r"ATD:\s*([A-Za-z0-9: ,\-]+UTC)")

    flag     = rex(r"(?:AIS Flag|Flag)\s*([A-Za-z &]+)")
    imo      = rex(r"IMO\s*/\s*MMSI\s*([0-9]{7,9})\s*/")
    callsign = rex(r"Callsign\s*([A-Za-z0-9\-]+)")
    mmsi     = rex(r"IMO\s*/\s*MMSI\s*[0-9]{7,9}\s*/\s*([0-9]{6,10})")

    return {
        "destination": destination,
        "course_deg": course_deg,
        "speed_kn": speed_kn,
        "draught_m": draught_m,
        "nav_status": nav_status,
        "position_age_min": position_age_min,
        "last_port_detailed": last_port_detailed,
        "atd_last_port_utc": atd_last_port_utc,
        "flag": flag,
        "imo": imo,
        "callsign": callsign,
        "mmsi_from_detail": mmsi,
    }

def enrich_with_detail_pages(df: pd.DataFrame, max_n: int = 12, per_page_timeout_ms: int = 12000) -> pd.DataFrame:
    if df.empty or "detail_url" not in df.columns:
        return df

    prio = ["in_port","incoming","outgoing","expected"]
    dfc = df.copy()
    dfc["__prio"] = dfc["status"].apply(lambda s: prio.index(s) if s in prio else 99)
    targets = dfc.dropna(subset=["detail_url"]).sort_values("__prio").head(max_n)

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
            if not isinstance(url, str) or not url.startswith("http"):
                continue
            try:
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(600)  # settle
                html = page.content()
                info = parse_detail_textual(html)

                course_deg = info["course_deg"]
                direction_cardinal = cardinal_from_degrees(course_deg) if course_deg is not None else None

                out_rows.append(dict(
                    mmsi=r["mmsi"],
                    detail_url=url,
                    destination=info["destination"],
                    last_port_detailed=info["last_port_detailed"],
                    atd_last_port_utc=info["atd_last_port_utc"],
                    course_deg=info["course_deg"],
                    heading_deg=None,
                    nav_status=info["nav_status"],
                    direction_cardinal=direction_cardinal,
                    position_age_min=info["position_age_min"],
                    draught_m=info["draught_m"],
                    flag=info["flag"],
                    imo=info["imo"],
                    callsign=info["callsign"],
                ))
                page.wait_for_timeout(250)
            except Exception:
                continue

        context.close()
        browser.close()

    if not out_rows:
        return df

    extra = pd.DataFrame(out_rows).drop_duplicates(subset=["mmsi"], keep="last")
    merged = df.merge(extra, on=["mmsi"], how="left", suffixes=("","_det"))

    # Prefer detail fields where present
    for col in ["destination","last_port_detailed","atd_last_port_utc","course_deg","heading_deg",
                "nav_status","direction_cardinal","position_age_min","draught_m","flag","imo","callsign","detail_url"]:
        merged[col] = merged[f"{col}_det"].combine_first(merged[col]) if f"{col}_det" in merged.columns else merged[col]

    drop_cols = [c for c in merged.columns if c.endswith("_det")]
    merged = merged.drop(columns=drop_cols, errors="ignore")
    return merged

# =========================
# Write outputs
# =========================
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
    df_in = dfx[(dfx["status"] == "in_port") & (~is_tug_series(dfx["ship_type"]))].copy()
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
    artifact_dir = os.path.join(os.getcwd(), "scrape_artifacts")
    tables = fetch_tables_with_headings(VF_URL, artifact_dir)
    print(f"✅ Found tables: {len(tables)}")

    df = normalize_concat(tables, artifact_dir)
    print(f"✅ Normalized rows: {len(df)} | cols: {list(df.columns)}")

    # Detail enrichment (polite cap)
    max_n = MAX_DETAIL_VESSELS if MAX_DETAIL_VESSELS > 0 else 0
    if max_n > 0:
        df = enrich_with_detail_pages(df, max_n=max_n)

    latest_key, hist_key, in_key = write_outputs(df)
    print(f"📝 latest:  s3://{S3_BUCKET}/{latest_key}")
    print(f"🗄️ history: s3://{S3_BUCKET}/{hist_key}")
    if in_key:
        print(f"📦 in-port: s3://{S3_BUCKET}/{in_key}")
    print(f"⏱️ Done in {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
