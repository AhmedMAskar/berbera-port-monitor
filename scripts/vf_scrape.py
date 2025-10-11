# scripts/vf_scrape.py
"""
Pull latest VesselFinder snapshot from Postgres and publish to S3.

Outputs locally:
- data/vf_snapshot.csv
- data/vf_snapshots/vf_snapshot_<TS>.csv

Uploads to S3 (if S3_BUCKET set via env/secrets):
- s3://<bucket>/<prefix>/latest/vf_snapshot.csv
- s3://<bucket>/<prefix>/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<TS>.csv

Env needed in the GitHub Action step:
- DATABASE_URL     (Postgres connection string)
- S3_BUCKET        (your bucket, e.g. berbera-port-monitor)
- S3_PREFIX        (optional; default 'berbera')
- AWS_REGION       (e.g. us-east-1)
"""

import os
import io
import time
import zlib
import datetime as dt
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# ------------------------------
# Config helpers
# ------------------------------
def get_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or "").strip()

DB_URL     = get_env("DATABASE_URL")
S3_BUCKET  = get_env("S3_BUCKET")
S3_PREFIX  = (get_env("S3_PREFIX") or "berbera").strip().strip("/")
AWS_REGION = get_env("AWS_REGION") or None

# ------------------------------
# Data source: Postgres
# ------------------------------
SQL_LATEST = """
WITH max_cap AS (SELECT MAX(captured_at) AS ts FROM vesselfinder_portcalls)
SELECT
  vessel_name,
  -- optional columns below may or may not exist in your table
  NULL::bigint AS mmsi,
  status,
  destination,
  eta_utc,
  captured_at
FROM vesselfinder_portcalls, max_cap
WHERE captured_at = max_cap.ts
ORDER BY status, COALESCE(eta_utc, captured_at), vessel_name;
"""

STATUS_MAP = {
    "expected":   "expected",
    "arrivals":   "incoming",
    "departures": "outgoing",
    "in_port":    "in_port",
}

def synth_id(name: str) -> int:
    """Stable integer from vessel name when MMSI is unavailable."""
    if not name:
        return 0
    return abs(zlib.crc32(name.encode("utf-8")))

def fetch_latest_df() -> pd.DataFrame:
    if not DB_URL:
        raise RuntimeError("DATABASE_URL is not set. Provide it in the GitHub Action env.")
    with psycopg2.connect(DB_URL) as conn:
        df = pd.read_sql(SQL_LATEST, conn)
    if df.empty:
        return df

    # Normalize schema expected by the app
    df = df.rename(columns={
        "vessel_name": "name",
        "eta_utc": "eta_to_berbera_utc",
    })

    # MMSI: if null, synthesize stable ID from vessel name
    if "mmsi" not in df.columns or df["mmsi"].isna().all():
        df["mmsi"] = df["name"].fillna("").map(synth_id)

    # Status mapping
    df["status"] = df["status"].astype(str).str.strip().str.lower().map(STATUS_MAP).fillna("unknown")

    # Ship type (unknown for now — can be enriched later)
    df["ship_type"] = "Unknown"

    # Add required columns (distance/last_port optional; leave blank for now)
    if "destination" in df.columns and "last_port" not in df.columns:
        df["last_port"] = df["destination"]
    if "distance_nm_to_berbera" not in df.columns:
        df["distance_nm_to_berbera"] = None
    if "speed_kn" not in df.columns:
        df["speed_kn"] = None

    # Timestamp fields
    df["scraped_at_utc"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "eta_to_berbera_utc" in df.columns:
        df["eta_to_berbera_utc"] = pd.to_datetime(df["eta_to_berbera_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Order & select columns for the CSV
    cols = ["mmsi","name","ship_type","status","last_port","distance_nm_to_berbera",
            "eta_to_berbera_utc","speed_kn","scraped_at_utc","captured_at"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]


# ------------------------------
# Write locally + upload to S3
# ------------------------------
def write_outputs(df: pd.DataFrame) -> tuple[Path, Path]:
    out_dir = Path("data") / "vf_snapshots"
    out_dir.mkdir(parents=True, exist_ok=True)

    ts_file = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    ts_csv = out_dir / f"vf_snapshot_{ts_file}.csv"
    latest_csv = Path("data") / "vf_snapshot.csv"

    df.to_csv(ts_csv, index=False)
    df.to_csv(latest_csv, index=False)

    print(f"📝 Wrote {ts_csv}")
    print(f"📝 Wrote {latest_csv}")
    return ts_csv, latest_csv

def s3_upload(local_file: Path, bucket: str, key: str) -> None:
    import boto3
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_file(str(local_file), bucket, key)
    print(f"✅ Uploaded: s3://{bucket}/{key}")

def upload_to_s3(ts_csv: Path, latest_csv: Path) -> None:
    if not S3_BUCKET:
        print("ℹ️ S3_BUCKET not set; skipping S3 upload.")
        return
    history_folder = dt.datetime.utcnow().strftime("%Y/%m/%d/%H%M")
    hist_key   = f"{S3_PREFIX}/history/csv/{history_folder}/{ts_csv.name}"
    latest_key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    s3_upload(ts_csv, S3_BUCKET, hist_key)
    s3_upload(latest_csv, S3_BUCKET, latest_key)

# ------------------------------
# Main
# ------------------------------
def main():
    print("🚀 Fetching latest VF snapshot from Postgres…")
    t0 = time.time()
    df = fetch_latest_df()
    print(f"✅ Retrieved {len(df)} rows in {time.time()-t0:.2f}s")

    if df.empty:
        print("⚠️ No rows found in vesselfinder_portcalls for the latest captured_at.")
    ts_csv, latest_csv = write_outputs(df)
    upload_to_s3(ts_csv, latest_csv)

if __name__ == "__main__":
    main()
