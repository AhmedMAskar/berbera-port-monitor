"""
Pull latest VesselFinder snapshot from Postgres and publish to S3.

Outputs locally:
- data/vf_snapshot.csv
- data/vf_snapshots/vf_snapshot_<TS>.csv

Uploads to S3 (if S3_BUCKET set via env/secrets):
- s3://<bucket>/<prefix>/latest/vf_snapshot.csv
- s3://<bucket>/<prefix>/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<TS>.csv

Required environment variables (GitHub Action step):
- DATABASE_URL     (Postgres connection string)
- S3_BUCKET        (your bucket, e.g. berbera-port-monitor)
- S3_PREFIX        (optional; default 'berbera')
- AWS_REGION       (e.g. us-east-1)
- AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (from IAM)
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


# ------------------------------------------------------------
# 1. Pre-flight validation for DATABASE_URL and S3 settings
# ------------------------------------------------------------
def get_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip().strip('"').strip("'")

RAW_DB = get_env("DATABASE_URL")
if not RAW_DB:
    raise SystemExit("❌ DATABASE_URL is missing. Add it to GitHub Actions secrets.")

bad_tokens = ["HOST", "USER", "PASSWORD", "DBNAME"]
if any(t in RAW_DB for t in bad_tokens):
    raise SystemExit(
        "❌ Your DATABASE_URL still contains placeholders "
        "(HOST / USER / PASSWORD / DBNAME). Please paste the real connection URI, "
        "e.g. postgresql://user:pass@hostname/dbname?sslmode=require"
    )

DB_URL     = RAW_DB
S3_BUCKET  = get_env("S3_BUCKET")
S3_PREFIX  = (get_env("S3_PREFIX") or "berbera").strip().strip("/")
AWS_REGION = get_env("AWS_REGION") or None


# ------------------------------------------------------------
# 2. SQL to pull the latest snapshot from your VF table
# ------------------------------------------------------------
SQL_LATEST = """
WITH max_cap AS (SELECT MAX(captured_at) AS ts FROM vesselfinder_portcalls)
SELECT
  vessel_name,
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


# ------------------------------------------------------------
# 3. Helpers
# ------------------------------------------------------------
def synth_id(name: str) -> int:
    """Stable integer from vessel name when MMSI unavailable."""
    if not name:
        return 0
    return abs(zlib.crc32(name.encode("utf-8")))


def fetch_latest_df() -> pd.DataFrame:
    print("🚀 Fetching latest VF snapshot from Postgres…")
    t0 = time.time()
    with psycopg2.connect(DB_URL) as conn:
        df = pd.read_sql(SQL_LATEST, conn)
    print(f"✅ Retrieved {len(df)} rows in {time.time() - t0:.2f}s")

    if df.empty:
        print("⚠️ No rows found for latest captured_at; exiting early.")
        return df

    # Normalize schema expected by Streamlit app
    df = df.rename(columns={"vessel_name": "name", "eta_utc": "eta_to_berbera_utc"})
    if "mmsi" not in df.columns or df["mmsi"].isna().all():
        df["mmsi"] = df["name"].fillna("").map(synth_id)
    df["status"] = (
        df["status"].astype(str).str.strip().str.lower().map(STATUS_MAP).fillna("unknown")
    )
    df["ship_type"] = "Unknown"
    df["last_port"] = df.get("destination", None)
    df["distance_nm_to_berbera"] = None
    df["speed_kn"] = None

    df["scraped_at_utc"] = pd.to_datetime(
        df["captured_at"], utc=True, errors="coerce"
    ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "eta_to_berbera_utc" in df.columns:
        df["eta_to_berbera_utc"] = pd.to_datetime(
            df["eta_to_berbera_utc"], utc=True, errors="coerce"
        ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    cols = [
        "mmsi",
        "name",
        "ship_type",
        "status",
        "last_port",
        "distance_nm_to_berbera",
        "eta_to_berbera_utc",
        "speed_kn",
        "scraped_at_utc",
        "captured_at",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]


# ------------------------------------------------------------
# 4. Write locally + upload to S3
# ------------------------------------------------------------
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
    hist_key = f"{S3_PREFIX}/history/csv/{history_folder}/{ts_csv.name}"
    latest_key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    s3_upload(ts_csv, S3_BUCKET, hist_key)
    s3_upload(latest_csv, S3_BUCKET, latest_key)


# ------------------------------------------------------------
# 5. Main entry point
# ------------------------------------------------------------
def main():
    df = fetch_latest_df()
    if df.empty:
        return
    ts_csv, latest_csv = write_outputs(df)
    upload_to_s3(ts_csv, latest_csv)


if __name__ == "__main__":
    main()
