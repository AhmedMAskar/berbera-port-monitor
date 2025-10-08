import os, io, time, csv
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2

URL = "https://www.vesselfinder.com/ports/SOBBO001"
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip().strip('"').strip("'")
S3_BUCKET     = (os.environ.get("S3_BUCKET") or "").strip()
AWS_ENABLED   = bool(S3_BUCKET)

def fetch_table():
    # polite headers
    r = requests.get(
        URL,
        headers={"User-Agent": "Mozilla/5.0 (educational demo; contact: your-email@example.com)"}, timeout=25
    )
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # VF often uses a params table with class "tparams"
    table = soup.find("table", {"class": "tparams"})
    if table is None:
        raise RuntimeError("Could not find data table on page. Layout may have changed.")

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) < 5:
            continue
        vessel_name = tds[0]
        shiptype    = tds[1]
        destination = tds[2]
        eta_raw     = tds[3]
        status      = tds[4]

        eta = None
        for fmt in ("%Y-%m-%d %H:%M", "%d %b %Y %H:%M", "%Y-%m-%d", "%d %b %Y"):
            try:
                eta = datetime.strptime(eta_raw, fmt).replace(tzinfo=timezone.utc)
                break
            except Exception:
                pass

        rows.append({
            "vessel_name": vessel_name or None,
            "shiptype":    shiptype or None,
            "destination": destination or None,
            "eta_utc":     eta.isoformat() if eta else None,
            "status":      status or None
        })
    return pd.DataFrame(rows)

def save_csv_local(df, captured_at):
    # timestamped filename
    ts = captured_at.strftime("%Y%m%d_%H%M%S")
    path = f"data/vf_snapshots/{captured_at:%Y}/{captured_at:%m}/{ts}.csv"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df_out = df.copy()
    df_out.insert(0, "captured_at", captured_at.isoformat())
    df_out.to_csv(path, index=False)
    return path

def upload_s3(local_path, s3_bucket, captured_at):
    import boto3
    s3 = boto3.client("s3")
    key = f"vf_snapshots/{captured_at:%Y}/{captured_at:%m}/{captured_at:%Y%m%d_%H%M%S}.csv"
    s3.upload_file(local_path, s3_bucket, key)
    return f"s3://{s3_bucket}/{key}"

def insert_db(df, captured_at):
    if not DATABASE_URL:
        print("No DATABASE_URL set — skipping DB insert.")
        return
    with psycopg2.connect(DATABASE_URL) as conn:
        conn.autocommit = True
        cur = conn.cursor()
        for row in df.itertuples(index=False):
            eta = None
            if getattr(row, "eta_utc", None):
                try:
                    eta = datetime.fromisoformat(row.eta_utc)
                except Exception:
                    eta = None
            cur.execute("""
                INSERT INTO vesselfinder_portcalls
                  (captured_at, vessel_name, shiptype, destination, eta_utc, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (captured_at, row.vessel_name, row.shiptype, row.destination, eta, row.status))
    print(f"Inserted {len(df)} rows into Neon.")

def main():
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    df = fetch_table()
    if df.empty:
        print("No rows parsed from page (empty table). Exiting.")
        return
    local_path = save_csv_local(df, captured_at)
    print(f"Saved CSV → {local_path}")

    if AWS_ENABLED:
        url = upload_s3(local_path, S3_BUCKET, captured_at)
        print(f"Uploaded to S3 → {url}")

    insert_db(df, captured_at)
    print("✅ Done.")

if __name__ == "__main__":
    main()
