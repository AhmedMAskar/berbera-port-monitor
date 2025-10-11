"""
Publish the latest *non-empty* VesselFinder snapshot from Postgres to S3.

Outputs locally:
- data/vf_snapshot.csv
- data/vf_snapshots/vf_snapshot_<TS>.csv

Uploads to S3:
- s3://<bucket>/<prefix>/latest/vf_snapshot.csv
- s3://<bucket>/<prefix>/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<TS>.csv

Env (set in GitHub Actions -> secrets):
- DATABASE_URL, S3_BUCKET, S3_PREFIX (default 'berbera'), AWS_REGION
- AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (for the workflow’s IAM user)
"""
import os, time, zlib, datetime as dt
from pathlib import Path
import pandas as pd
import psycopg2

# -------------------- Preflight --------------------
def env(name, default=""):
    return (os.getenv(name) or default).strip().strip('"').strip("'")

RAW_DB = env("DATABASE_URL")
if not RAW_DB:
    raise SystemExit("❌ DATABASE_URL is missing in GitHub Secrets.")
if any(tok in RAW_DB for tok in ["HOST","USER","PASSWORD","DBNAME"]):
    raise SystemExit("❌ DATABASE_URL still has placeholders. Paste the real URI (…?sslmode=require).")

DB_URL     = RAW_DB
S3_BUCKET  = env("S3_BUCKET")
S3_PREFIX  = (env("S3_PREFIX") or "berbera").strip().strip("/")
AWS_REGION = env("AWS_REGION") or None

# -------------------- SQL --------------------
# 1) Rank snapshots by how informative they are (prefer snapshots that include
#    any of arrivals/departures/expected). Fall back to most recent non-empty.
SQL_PICK = """
WITH agg AS (
  SELECT
    captured_at,
    COUNT(*)                                         AS total_rows,
    SUM((status='in_port')::int)                     AS in_port_rows,
    SUM((status='arrivals')::int)                    AS arrivals_rows,
    SUM((status='departures')::int)                  AS departures_rows,
    SUM((status='expected')::int)                    AS expected_rows
  FROM vesselfinder_portcalls
  GROUP BY captured_at
),
ranked AS (
  SELECT
    captured_at,
    total_rows,
    in_port_rows,
    arrivals_rows,
    departures_rows,
    expected_rows,
    -- prefer snapshots that have any movement-related rows, then newest
    (CASE WHEN (arrivals_rows + departures_rows + expected_rows) > 0 THEN 1 ELSE 0 END) AS has_movement
  FROM agg
  WHERE total_rows > 0
)
SELECT captured_at
FROM ranked
ORDER BY has_movement DESC, captured_at DESC
LIMIT 1;
"""

SQL_COUNTS = """
SELECT status, COUNT(*) AS rows
FROM vesselfinder_portcalls
WHERE captured_at = %s
GROUP BY status
ORDER BY status;
"""

SQL_EXPORT = """
SELECT
  vessel_name,
  NULL::bigint AS mmsi,
  status,
  destination,
  eta_utc,
  captured_at
FROM vesselfinder_portcalls
WHERE captured_at = %s
ORDER BY status, COALESCE(eta_utc, captured_at), vessel_name;
"""

STATUS_MAP = {
    "expected":   "expected",
    "arrivals":   "incoming",
    "departures": "outgoing",
    "in_port":    "in_port",
}

def synth_id(name: str) -> int:
    return abs(zlib.crc32((name or "").encode("utf-8")))

# -------------------- Core --------------------
def fetch_best_snapshot_df() -> pd.DataFrame:
    with psycopg2.connect(DB_URL) as conn, conn.cursor() as cur:
        cur.execute(SQL_PICK)
        row = cur.fetchone()
        if not row or not row[0]:
            print("⚠️ No non-empty snapshots found in vesselfinder_portcalls.")
            return pd.DataFrame()
        picked = row[0]
        print(f"📌 Using captured_at = {picked} (best recent non-empty snapshot)")

        # Print counts per status for visibility in logs
        df_counts = pd.read_sql(SQL_COUNTS, conn, params=[picked])
        if not df_counts.empty:
            print("\n=== Rows by status for chosen snapshot ===")
            print(df_counts.to_string(index=False))

        df = pd.read_sql(SQL_EXPORT, conn, params=[picked])

    # Normalize schema expected by app
    if df.empty:
        return df
    df = df.rename(columns={"vessel_name": "name", "eta_utc": "eta_to_berbera_utc"})
    if "mmsi" not in df.columns or df["mmsi"].isna().all():
        df["mmsi"] = df["name"].map(synth_id)
    df["status"] = df["status"].astype(str).str.strip().str.lower().map(STATUS_MAP).fillna("unknown")
    df["ship_type"] = "Unknown"
    df["last_port"] = df.get("destination")
    df["distance_nm_to_berbera"] = None
    df["speed_kn"] = None

    df["scraped_at_utc"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "eta_to_berbera_utc" in df.columns:
        df["eta_to_berbera_utc"] = pd.to_datetime(df["eta_to_berbera_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    cols = ["mmsi","name","ship_type","status","last_port","distance_nm_to_berbera",
            "eta_to_berbera_utc","speed_kn","scraped_at_utc","captured_at"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]

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
    s3_hist   = f"{S3_PREFIX}/history/csv/{history_folder}/{ts_csv.name}"
    s3_latest = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    s3_upload(ts_csv, S3_BUCKET, s3_hist)
    s3_upload(latest_csv, S3_BUCKET, s3_latest)

def main():
    t0 = time.time()
    df = fetch_best_snapshot_df()
    print(f"⏱️ Query + normalize took {time.time()-t0:.2f}s")
    if df.empty:
        raise SystemExit("❌ Chosen snapshot produced 0 rows. Nothing to upload.")
    ts_csv, latest_csv = write_outputs(df)
    upload_to_s3(ts_csv, latest_csv)

if __name__ == "__main__":
    main()
