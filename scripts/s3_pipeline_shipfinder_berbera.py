# scripts/s3_pipeline_shipfinder_berbera.py
"""
Berbera Port Monitor — VesselFinder → S3 pipeline (hardened)
- Fails fast when empty (no silent empty CSVs)
- Optional DEBUG: save HTML to S3 when tables not found
- Logs status counts + upload keys
- Normalizes "Berbera, Somaliland"
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

ALLOW_EMPTY_UPLOAD       = get_env("ALLOW_EMPTY_UPLOAD", "0") in ("1","true","True")
DEBUG_SAVE_HTML_TO_S3    = get_env("DEBUG_SAVE_HTML_TO_S3", "0") in ("1","true","True")

if not S3_BUCKET:
    raise SystemExit("❌ S3_BUCKET is required.")

# =========================
# Constants / UA / TEU
# =========================
STATUS_FROM_HEADING = {
    "arrivals": "incoming", "arrival": "incoming", "incoming": "incoming",
    "departures": "outgoing","departure": "outgoing","outgoing": "outgoing",
    "expected": "expected",
    "in port": "in_port","in-port": "in_port","inport": "in_port",
}

APP_COLS = [
    "name","mmsi","imo","callsign",
    "ship_type","status","last_port","destination",
    "speed_kn","course_deg","heading_deg","draught_m",
    "gt","dwt","length_m","beam_m","built_year",
    "eta_to_berbera_utc","atd_last_port_utc","scraped_at_utc","source",
    "teu_capacity_actual","teu_equiv",
    "detail_url","last_port_detailed","nav_status","position_age_min","flag",
    "lat_deg","lon_deg","id_source_imo","id_source_mmsi",
    "synth_id",
]

REAL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

TEU_PER_TON = 1/12.0
K_LxB       = 0.50
INCLUDE_PASSENGER_AS_TEU = False

# =========================
# Helpers
# =========================
def s3() -> boto3.client:
    return boto3.client("s3", region_name=AWS_REGION)

def put_bytes(bucket: str, key: str, data: bytes, content_type="text/plain"):
    s3().put_object(
        Bucket=bucket, Key=key, Body=data, ContentType=content_type,
        CacheControl="no-cache, no-store, max-age=0, must-revalidate",
    )
    print(f"✅ Uploaded: s3://{bucket}/{key} (no-cache)")

def put_csv(bucket: str, key: str, csv_bytes: bytes):
    put_bytes(bucket, key, csv_bytes, content_type="text/csv")

def synth_id(name: str) -> int:
    return 0 if not name else abs(zlib.crc32(name.encode("utf-8")))

def now_utc_str() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def to_iso_utc_or_none(txt: Optional[str]) -> Optional[str]:
    if not txt or not str(txt).strip():
        return None
    ts = pd.to_datetime(txt, errors="coerce", utc=True)
    if ts is pd.NaT or pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

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

def _fix_berbera(txt: Optional[str]) -> Optional[str]:
    if not isinstance(txt, str) or not txt.strip():
        return txt
    out = re.sub(r"\bberbera\s*,?\s*somalia\b", "Berbera, Somaliland", txt, flags=re.IGNORECASE)
    out = re.sub(r"\bberbera\b", "Berbera", out, flags=re.IGNORECASE)
    return out

# =========================
# HTML table parsing
# =========================
def heading_to_status(h: str) -> Optional[str]:
    h = (h or "").lower().strip()
    for k, v in STATUS_FROM_HEADING.items():
        if k in h: return v
    return None

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

        # try cookie banners
        for sel in [
            "button:has-text('Accept')","button:has-text('I Agree')","button:has-text('Agree')",
            "text=Accept all","text=Accept All","[id*='accept']","[class*='accept']","[aria-label*='accept']",
        ]:
            try:
                loc = page.locator(sel).first
                if loc.is_visible():
                    loc.click(timeout=1000)
                    page.wait_for_timeout(400)
                    break
            except Exception:
                pass

        # scroll to trigger lazy content
        for _ in range(6):
            page.mouse.wheel(0, 1400)
            page.wait_for_timeout(650)

        try:
            page.wait_for_selector("table", state="visible", timeout=8000)
        except PWTimeout:
            if DEBUG_SAVE_HTML_TO_S3:
                html_key = f"{S3_PREFIX}/debug/html/{dt.datetime.utcnow():%Y/%m/%d/%H%M}/port_page.html"
                put_bytes(S3_BUCKET, html_key, page.content().encode("utf-8"), content_type="text/html")
                print(f"🧪 Saved debug HTML → s3://{S3_BUCKET}/{html_key}")

        # main + iframes
        results.extend(_collect_tables_from_context(page))
        for fr in page.frames:
            if fr == page.main_frame: continue
            try:
                if fr.locator("table").count() > 0:
                    results.extend(_collect_tables_from_context(fr))
            except Exception:
                continue

        context.close()
        browser.close()

    print(f"🔎 Total tables captured: {len(results)}")
    return results

def _collect_tables_from_context(ctx) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
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
                out.append((heading_text.lower(), table_html))
        except Exception:
            continue
    return out

def parse_table(table_html: str, heading_text: str) -> pd.DataFrame:
    soup = BeautifulSoup(table_html, "html.parser")
    table = soup.find("table")
    if table is None:
        return pd.DataFrame(columns=APP_COLS)

    # headers
    header_cells = []
    thead = table.find("thead")
    if thead:
        header_cells = [th.get_text(strip=True) for th in thead.find_all("th")]
    if not header_cells:
        first_tr = table.find("tr")
        if first_tr:
            header_cells = [td.get_text(strip=True) for td in first_tr.find_all(["th","td"])]

    if not header_cells:
        return pd.DataFrame(columns=APP_COLS)

    def norm(s): return (s or "").strip().lower()
    name_col_idx = None
    for i, h in enumerate(header_cells):
        if norm(h) in ("vessel","ship name","ship","name"):
            name_col_idx = i; break
    if name_col_idx is None:
        return pd.DataFrame(columns=APP_COLS)

    trs = table.find_all("tr")
    start_row = 1 if trs and [td.get_text(strip=True).lower() for td in trs[0].find_all(["th","td"])] == [h.lower() for h in header_cells] else 0

    rows_out = []
    for tr in trs[start_row:]:
        tds = tr.find_all(["td","th"])
        if not tds:
            continue
        if len(tds) < len(header_cells):
            for _ in range(len(header_cells) - len(tds)):
                empty_td = soup.new_tag("td")
                tds.append(empty_td)

        cells_text = [td.get_text(strip=True) for td in tds[:len(header_cells)]]

        # vessel hyperlink from NAME cell
        name_cell = tds[name_col_idx]
        a = name_cell.find("a", href=True)
        href = a["href"].strip() if a else None
        if href and not href.startswith("http"):
            detail_url = "https://www.vesselfinder.com" + href
        else:
            detail_url = href

        # IMO from URL, if present
        imo_from_url = None
        if detail_url:
            m = re.search(r"/IMO\s*([0-9]{7})", detail_url, flags=re.I) or re.search(r"imo=([0-9]{7})", detail_url, flags=re.I)
            if m: imo_from_url = m.group(1)

        # header index helpers
        def find_idx(patterns):
            for i, h in enumerate(header_cells):
                for p in patterns:
                    if p in h.lower(): return i
            return None

        idx_type  = find_idx(["type","ship type"])
        idx_from  = find_idx(["from","origin","last port","previous","prev port"])
        idx_to    = find_idx(["to","destination","dest","next port","port of call"])
        idx_eta   = find_idx(["eta","arrives","arrival","atd/eta"])
        idx_speed = find_idx(["speed","kn","knots"])
        idx_gt    = find_idx(["gt","gross"])
        idx_dwt   = find_idx(["dwt","deadweight"])
        idx_size  = find_idx(["size","size (m)","length / beam","length/beam"])
        idx_built = find_idx(["built","year built"])

        name  = cells_text[name_col_idx] if name_col_idx < len(cells_text) else None
        ship_type = cells_text[idx_type] if idx_type is not None and idx_type < len(cells_text) else "Unknown"

        last_port = None
        if idx_from is not None and idx_from < len(cells_text):
            last_port = cells_text[idx_from]
        elif idx_to is not None and idx_to < len(cells_text):
            last_port = cells_text[idx_to]
        last_port = _fix_berbera(last_port)

        eta_txt = cells_text[idx_eta] if idx_eta is not None and idx_eta < len(cells_text) else None
        speed_txt = cells_text[idx_speed] if idx_speed is not None and idx_speed < len(cells_text) else None
        gt_txt = cells_text[idx_gt] if idx_gt is not None and idx_gt < len(cells_text) else None
        dwt_txt = cells_text[idx_dwt] if idx_dwt is not None and idx_dwt < len(cells_text) else None
        size_txt = cells_text[idx_size] if idx_size is not None and idx_size < len(cells_text) else None
        built_txt = cells_text[idx_built] if idx_built is not None and idx_built < len(cells_text) else None

        speed_kn = None
        if speed_txt:
            m = re.findall(r"([0-9]+(?:\.[0-9]+)?)", speed_txt)
            if m:
                try: speed_kn = float(m[0])
                except: pass

        gt  = pd.to_numeric(re.sub(r"[^\d.]", "", gt_txt or ""), errors="coerce")
        dwt = pd.to_numeric(re.sub(r"[^\d.]", "", dwt_txt or ""), errors="coerce")
        L = B = None
        if size_txt:
            m = re.findall(r"(\d+(?:\.\d+)?)", size_txt)
            if len(m) >= 2:
                try:
                    L = float(m[0]); B = float(m[1])
                except: pass
        built_year = pd.to_numeric(re.sub(r"[^\d]", "", (built_txt or "")), errors="coerce")

        rows_out.append({
            "name": (name or "").strip(),
            "detail_url": detail_url,
            "imo": imo_from_url,
            "mmsi": None,
            "ship_type": (ship_type or "Unknown").strip().title(),
            "last_port": (last_port or None),
            "eta_to_berbera_utc": to_iso_utc_or_none(eta_txt),
            "speed_kn": speed_kn,
            "gt": gt, "dwt": dwt, "length_m": L, "beam_m": B, "built_year": built_year,
            "scraped_at_utc": now_utc_str(),
            "source": "vesselfinder",
            "status": (heading_to_status(heading_text) or "unknown"),
            "teu_capacity_actual": None,
        })

    out_df = pd.DataFrame(rows_out).replace({pd.NA: None})
    out_df["synth_id"] = out_df["name"].fillna("").map(synth_id)
    if not out_df.empty:
        out_df["ship_type"] = out_df["ship_type"].astype(str).str.strip().str.title()
        out_df["status"]    = out_df["status"].astype(str).str.strip().str.lower()
    return out_df

def normalize_concat(tables: List[Tuple[str, str]]) -> pd.DataFrame:
    if not tables:
        return pd.DataFrame(columns=APP_COLS)
    frames: List[pd.DataFrame] = [parse_table(html, heading) for heading, html in tables]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=APP_COLS)
    df = pd.concat(frames, ignore_index=True)
    if not df.empty:
        df = df.dropna(subset=["detail_url"]).drop_duplicates(subset=["detail_url"], keep="last")
    return df

# =========================
# Detail page enrichment
# =========================
def parse_detail_textual(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    def rex(pattern, group=1, flags=0):
        m = re.search(pattern, text, flags)
        return m.group(group).strip() if m else None

    imo = rex(r"(?:IMO)\s*[/ ]\s*([0-9]{7})", flags=re.I) or rex(r"IMO\s*([0-9]{7})", flags=re.I)
    mmsi = rex(r"(?:MMSI)\s*[/ ]\s*([0-9]{9})", flags=re.I) or rex(r"MMSI\s*([0-9]{9})", flags=re.I)
    id_src_imo = "voyage_text" if imo else None
    id_src_mmsi = "voyage_text" if mmsi else None

    if not (imo and mmsi):
        for script in soup.find_all("script", {"type":"application/ld+json"}):
            try:
                j = script.string or ""
                m_imo = re.search(r'"IMO"\s*:\s*"([0-9]{7})"', j) or re.search(r'"imo"\s*:\s*"([0-9]{7})"', j, re.I)
                m_mmsi = re.search(r'"MMSI"\s*:\s*"([0-9]{9})"', j) or re.search(r'"mmsi"\s*:\s*"([0-9]{9})"', j, re.I)
                if (not imo) and m_imo:  imo = m_imo.group(1); id_src_imo = "jsonld"
                if (not mmsi) and m_mmsi: mmsi = m_mmsi.group(1); id_src_mmsi = "jsonld"
            except Exception:
                pass

    if not imo:
        m = re.search(r'\bIMO\s*([0-9]{7})\b', html, re.I)
        if m: imo = m.group(1); id_src_imo = id_src_imo or "html_fallback"
    if not mmsi:
        m = re.search(r'\bMMSI\s*([0-9]{9})\b', html, re.I)
        if m: mmsi = m.group(1); id_src_mmsi = id_src_mmsi or "html_fallback"

    destination = rex(r"(?:Destination|DESTINATION)\s+([A-Za-z0-9 ,\-/()]+)")
    last_port_detailed = rex(r"(?:Last Port|Previous port|From|Origin)\s*([A-Za-z0-9 ,\-/()&]+)")
    atd_last_port_utc  = rex(r"ATD:\s*([A-Za-z0-9: ,\-]+UTC)")
    nav_status = rex(r"(?:Navigation Status|Status)\s*([A-Za-z \-/]+)")
    draught_m = rex(r"Current draught\s*([0-9]+(?:\.[0-9]+)?)\s*m")
    draught_m = float(draught_m) if draught_m else None

    course_deg = rex(r"Course\s*/\s*Speed\s*([0-9]+(?:\.[0-9]+)?)\s*°")
    speed_kn   = rex(r"Course\s*/\s*Speed\s*[0-9]+(?:\.[0-9]+)?\s*°\s*/\s*([0-9]+(?:\.[0-9]+)?)\s*kn")
    course_deg = float(course_deg) if course_deg else None
    speed_kn   = float(speed_kn) if speed_kn else None

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

    lat_deg = lon_deg = None
    cand = soup.find(attrs={"data-lat": True, "data-lon": True}) or soup.find(attrs={"data-latitude": True, "data-longitude": True})
    if cand:
        try:
            lat_deg = float(cand.get("data-lat") or cand.get("data-latitude"))
            lon_deg = float(cand.get("data-lon") or cand.get("data-longitude"))
        except Exception:
            pass
    if lat_deg is None or lon_deg is None:
        m = re.search(r'"lat(?:itude)?"\s*:\s*(-?\d+(?:\.\d+)?)\s*,\s*"lon(?:gitude)?"\s*:\s*(-?\d+(?:\.\d+)?)\s*', html, re.I)
        if m:
            lat_deg = float(m.group(1)); lon_deg = float(m.group(2))
    if lat_deg is None or lon_deg is None:
        m = re.search(r"L\.(?:marker|circle)\(\s*\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\]", html)
        if m:
            lat_deg = float(m.group(1)); lon_deg = float(m.group(2))

    callsign = rex(r"Callsign\s*([A-Za-z0-9\-]+)")
    flag     = rex(r"(?:AIS Flag|Flag)\s*([A-Za-z &]+)")

    destination = _fix_berbera(destination)
    last_port_detailed = _fix_berbera(last_port_detailed)

    return {
        "imo": imo, "mmsi": mmsi,
        "id_source_imo": id_src_imo, "id_source_mmsi": id_src_mmsi,
        "destination": destination, "last_port_detailed": last_port_detailed, "atd_last_port_utc": atd_last_port_utc,
        "nav_status": nav_status, "draught_m": draught_m,
        "course_deg": course_deg, "speed_kn": speed_kn, "position_age_min": position_age_min,
        "lat_deg": lat_deg, "lon_deg": lon_deg, "flag": flag,
    }

def _backoff_sleep(attempt: int):
    base = DETAIL_BACKOFF_BASE_MS
    cap  = DETAIL_BACKOFF_MAX_MS
    delay = min(cap, base * (2 ** (attempt - 1)))
    delay = random.randint(int(delay * 0.5), delay)
    time.sleep(delay / 1000.0)

def enrich_with_detail_pages(df: pd.DataFrame, max_n: int = 60, per_page_timeout_ms: int = 15000) -> pd.DataFrame:
    if df.empty or "detail_url" not in df.columns:
        return df

    prio_order = {"in_port":0, "incoming":1, "expected":2, "outgoing":3}
    dfc = df.copy()
    dfc["__prio"] = dfc["status"].map(lambda s: prio_order.get(str(s).lower(), 99))

    targets = (dfc.dropna(subset=["detail_url"])
                 .sort_values(["__prio","name"])
                 .drop_duplicates(subset=["detail_url"], keep="last")
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
            if not isinstance(url, str) or not url.startswith("http"):
                continue

            for attempt in range(1, DETAIL_RETRIES + 1):
                try:
                    page.goto(url, wait_until="domcontentloaded")
                    page.wait_for_timeout(300)
                    html = page.content()
                    info = parse_detail_textual(html)
                    out_rows.append({**info, "detail_url": url})
                    break
                except Exception as e:
                    print(f"⚠️ detail attempt {attempt}/{DETAIL_RETRIES}: {url} — {e}")
                    if attempt < DETAIL_RETRIES:
                        _backoff_sleep(attempt)
                        continue
            if DETAIL_SLEEP_BETWEEN_MS > 0:
                time.sleep(DETAIL_SLEEP_BETWEEN_MS / 1000.0)

        context.close()
        browser.close()

    if not out_rows:
        return df

    extra = pd.DataFrame(out_rows)
    if "mmsi" in extra.columns:
        extra.loc[~extra["mmsi"].astype(str).str.fullmatch(r"\d{9}", na=False), "mmsi"] = None
    if "imo" in extra.columns:
        extra.loc[~extra["imo"].astype(str).str.fullmatch(r"\d{7}", na=False), "imo"] = None

    merged = df.merge(extra, on=["detail_url"], how="left", suffixes=("","_det"))

    prefer_cols = [
        "mmsi","imo","callsign","destination","last_port_detailed","atd_last_port_utc",
        "course_deg","speed_kn","nav_status","position_age_min","draught_m","flag",
        "lat_deg","lon_deg","id_source_imo","id_source_mmsi"
    ]
    for col in prefer_cols:
        det = f"{col}_det"
        if det in merged.columns:
            merged[col] = merged[det].combine_first(merged[col])

    for c in ["destination", "last_port_detailed", "last_port"]:
        if c in merged.columns:
            merged[c] = merged[c].map(_fix_berbera)

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

def write_outputs(df: pd.DataFrame) -> Tuple[str, Optional[str], Optional[str]]:
    for c in ["destination","last_port","last_port_detailed"]:
        if c in df.columns:
            df[c] = df[c].map(_fix_berbera)

    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    latest_key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    hist_prefix = dt.datetime.utcnow().strftime(f"{S3_PREFIX}/history/csv/%Y/%m/%d/%H%M")
    hist_key = f"{hist_prefix}/vf_snapshot_{ts}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    put_csv(S3_BUCKET, latest_key, csv_bytes)
    put_csv(S3_BUCKET, hist_key, csv_bytes)

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
    tables = fetch_tables_with_headings(VF_URL)
    if not tables:
        print("❌ No tables found on port page.")
        if not ALLOW_EMPTY_UPLOAD:
            raise SystemExit(2)
    df = normalize_concat(tables)
    print(f"✅ Normalized rows: {len(df)}")

    if df.empty:
        print("❌ Parsed 0 rows from all tables.")
        if not ALLOW_EMPTY_UPLOAD:
            raise SystemExit(3)

    if not df.empty:
        df = enrich_with_detail_pages(df, max_n=MAX_DETAIL_VESSELS)
    df = compute_teu(df)

    if not df.empty and "status" in df.columns:
        counts = df["status"].value_counts(dropna=False).to_dict()
        print(f"📊 Status counts: {counts}")

    latest_key, hist_key, in_key = write_outputs(df)
    print(f"📝 latest:  s3://{S3_BUCKET}/{latest_key}")
    print(f"🗄️ history: s3://{S3_BUCKET}/{hist_key}")
    if in_key:
        print(f"📦 in-port: s3://{S3_BUCKET}/{in_key}")
    print(f"⏱️ Done in {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
