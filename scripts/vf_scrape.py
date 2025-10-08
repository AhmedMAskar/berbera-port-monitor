# scripts/vf_scrape.py
import os
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2

URL = "https://www.vesselfinder.com/ports/SOBBO001"
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip().strip('"').strip("'")

# Berbera local time = EAT (UTC+3)
PORT_TZ = timezone(timedelta(hours=3))

SECTIONS = {
    "Expected ships in Berbera": "expected",
    "Recent ship arrivals in Berbera": "arrivals",
    "Recent ship departures from Berbera": "departures",
    "Ships in port": "in_port",
}

def _clean_txt(x: str | None) -> str | None:
    if x is None:
        return None
    x = " ".join(str(x).split())
    return x or None

def parse_port_time(txt: str | None) -> datetime | None:
    """Parse strings like 'Oct 10, 10:00' or 'Oct 8, 08:49' (port local time)."""
    if not txt:
        return None
    txt = _clean_txt(txt)
    # inject current year if missing
    now = datetime.now(PORT_TZ)
    candidates = []
    # common formats
    fmts = [
        "%b %d, %H:%M",        # 'Oct 10, 10:00'
        "%b %d, %Y %H:%M",     # 'Oct 10, 2025 10:00'
        "%Y-%m-%d %H:%M",      # '2025-10-10 10:00'
        "%d %b %Y %H:%M",      # '10 Oct 2025 10:00'
        "%d %b %H:%M",         # '10 Oct 10:00' (rare)
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(txt, f)
            # if year not in string, add current year
            if "%Y" not in f:
                dt = dt.replace(year=now.year)
            # interpret as port local time, convert to UTC
            dt = dt.replace(tzinfo=PORT_TZ).astimezone(timezone.utc)
            return dt
        except Exception:
            candidates.append(f)
            continue
    return None  # couldn't parse

def fetch_sections() -> list[pd.DataFrame]:
    # Polite, browser-ish headers to get the full server-side HTML
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/127.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Referer": "https://www.vesselfinder.com/ports",
        "Connection": "close",
    }
    r = requests.get(URL, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")

    dataframes: list[pd.DataFrame] = []

    # walk each H2 section we care about; grab the very next <table>
    for h2 in soup.find_all("h2"):
        title = _clean_txt(h2.get_text())
        if title not in SECTIONS:
            continue
        section = SECTIONS[title]
        table = h2.find_next("table")
        if table is None:
            # no table found under this section; skip
            continue

        # Use pandas parser on just this table’s HTML
        try:
            df = pd.read_html(str(table))[0]
        except Exception:
            # fallback: try to build a dataframe manually (last resort)
            rows = []
            for tr in table.find_all("tr"):
                rows.append([_clean_txt(td.get_text()) for td in tr.find_all("td")])
            if not rows:
                continue
            df = pd.DataFrame(rows[1:], columns=rows[0])

        # Normalize columns we care about
        cols = {c: c.strip() for c in df.columns}
        df.rename(columns=cols, inplace=True)

        # unify into expected schema
        # different tables have different first-time columns:
        #   Expected: 'ETA'
        #   Arrivals: 'Arrival (LT)'
        #   Departures: 'Departure (LT)'
        #   In Port: 'Last report'
        when_col = None
        for c in df.columns:
            if c.lower().startswith(("eta", "arrival", "departure", "last report")):
                when_col = c
                break

        vessel_col = None
        for c in df.columns:
            if c.lower().startswith("vessel"):
                vessel_col = c
                break

        dest_col = None
        for c in df.columns:
            if c.lower().startswith("destination"):
                dest_col = c
                break

        # build output rows
        out = pd.DataFrame({
            "vessel_name": df[vessel_col].map(_clean_txt) if vessel_col else None,
            "shiptype":    None,  # optional: try to split type out of vessel_col later
            "destination": df[dest_col].map(_clean_txt) if dest_col in df else None,
            "eta_utc":     pd.to_datetime(
                               df[when_col].map(parse_port_time),
                               utc=True, errors="coerce"
                           ) if when_col else None,
            "status":      section,
        })

        # keep only non-empty vessel rows
        out = out[out["vessel_name"].notna()].copy()
        if not out.empty:
            dataframes.append(out)

    return dataframes

def insert_db(df_all: pd.DataFrame, captured_at_utc: datetime):
    if not DATABASE_URL:
        print("No DATABASE_URL set — skipping DB insert.")
        return
    with psycopg2.connect(DATABASE_URL) as conn:
        conn.autocommit = True
        cur = conn.cursor()
        for row in df_all.itertuples(index=False):
            eta = None
            if getattr(row, "eta_utc", None) and pd.notna(row.eta_utc):
                eta = row.eta_utc.to_pydatetime()
            cur.execute("""
                INSERT INTO vesselfinder_portcalls
                  (captured_at, vessel_name, shiptype, destination, eta_utc, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (captured_at_utc, row.vessel_name, row.shiptype, row.destination, eta, row.status))

def save_csv_local(df_all: pd.DataFrame, captured_at_utc: datetime) -> str:
    ts = captured_at_utc.strftime("%Y%m%d_%H%M%S")
    path = f"data/vf_snapshots/{captured_at_utc:%Y}/{captured_at_utc:%m}/{ts}.csv"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = df_all.copy()
    tmp.insert(0, "captured_at", captured_at_utc.isoformat())
    tmp.to_csv(path, index=False)
    return path

def main():
    # 1) fetch and parse all sections
    dfs = fetch_sections()
    if not dfs:
        raise RuntimeError("No tables parsed from the page. Structure may have changed or access was blocked.")
    df_all = pd.concat(dfs, ignore_index=True)

    # 2) timestamp
    captured_at_utc = datetime.now(timezone.utc).replace(microsecond=0)

    # 3) save CSV locally (artifact)
    local_path = save_csv_local(df_all, captured_at_utc)
    print(f"Saved CSV → {local_path} (rows: {len(df_all)})")

    # 4) insert into Neon
    insert_db(df_all, captured_at_utc)
    print(f"Inserted {len(df_all)} rows into Neon.")

if __name__ == "__main__":
    main()
