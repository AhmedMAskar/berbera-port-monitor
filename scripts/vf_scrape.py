"""
VesselFinder scraper runner (schedule is controlled by GitHub Actions YAML).

What this script does:
- Runs your scraping function (replace the TODO block with your real logic).
- Writes a timestamped CSV into data/vf_snapshots/
- Also writes/overwrites data/vf_snapshots/latest.csv
- Optionally uploads to S3 if S3_BUCKET is set (requires boto3 in requirements).

Env vars expected (via GitHub Actions secrets or your local env):
- DATABASE_URL  (optional – if your scraper uses a database)
- S3_BUCKET     (optional – for S3 upload)
"""

import os
import csv
import time
import datetime as dt
from pathlib import Path

try:
    import pandas as pd  # recommended; add to scripts/requirements.txt
except Exception:
    pd = None

# Optional S3 upload (requires boto3 in scripts/requirements.txt)
def _try_s3_upload(local_path: Path, bucket: str, s3_key: str) -> None:
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.upload_file(str(local_path), bucket, s3_key)
        print(f"✅ Uploaded to s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"⚠️ S3 upload skipped/failed: {e}")

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def scrape_vesselfinder():
    """
    TODO: Replace this stub with your real scraping logic.
    Must return a list of dicts (or a pandas DataFrame) with consistent columns.
    Example columns: ts_utc, vessel_name, imo, status, yard, source
    """
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows = [
        {
            "ts_utc": now,
            "vessel_name": "Demo Vessel",
            "imo": "1234567",
            "status": "scrapped",
            "yard": "Demo Yard",
            "source": "VesselFinder",
        }
    ]
    return rows

def to_dataframe(data):
    """Normalize to pandas DataFrame (or CSV rows) without breaking if pandas missing."""
    if pd is not None:
        return pd.DataFrame(data)
    return data  # will be handled by csv module

def write_outputs(df_or_rows, out_dir: Path) -> tuple[Path, Path]:
    """Write timestamped CSV and latest.csv. Returns (timestamped_path, latest_path)."""
    _ensure_dir(out_dir)
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    ts_csv = out_dir / f"vf_{ts}.csv"
    latest_csv = out_dir / "latest.csv"

    if pd is not None and isinstance(df_or_rows, pd.DataFrame):
        df_or_rows.to_csv(ts_csv, index=False)
        df_or_rows.to_csv(latest_csv, index=False)
    else:
        # Fallback: plain csv module
        rows = df_or_rows
        if not rows:
            # write empty with a minimal header
            with open(ts_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["ts_utc", "vessel_name", "imo", "status", "yard", "source"])
            latest_csv.write_text(ts_csv.read_text(encoding="utf-8"), encoding="utf-8")
            return ts_csv, latest_csv
        # Write with inferred headers
        headers = sorted(rows[0].keys())
        with open(ts_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            w.writerows(rows)
        # Copy to latest
        latest_csv.write_text(ts_csv.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"📝 Wrote {ts_csv}")
    print(f"📝 Wrote {latest_csv}")
    return ts_csv, latest_csv

def maybe_upload_s3(paths: list[Path], bucket: str | None) -> None:
    if not bucket:
        print("ℹ️ S3_BUCKET not set; skipping S3 upload.")
        return
    for p in paths:
        key = f"vf_snapshots/{p.name}"
        _try_s3_upload(p, bucket, key)

def main():
    # Inputs
    database_url = os.getenv("DATABASE_URL", "")
    s3_bucket = os.getenv("S3_BUCKET", "")

    if database_url:
        print("🔗 DATABASE_URL is set (hidden).")
    else:
        print("ℹ️ DATABASE_URL not set or not required.")

    # Scrape
    t0 = time.time()
    print("🚀 Starting VesselFinder scrape...")
    data = scrape_vesselfinder()
    df_or_rows = to_dataframe(data)
    elapsed = time.time() - t0
    print(f"✅ Scrape complete in {elapsed:.2f}s. Rows: {len(data) if hasattr(data, '__len__') else 'n/a'}")

    # Write outputs
    out_dir = Path("data") / "vf_snapshots"
    ts_csv, latest_csv = write_outputs(df_or_rows, out_dir)

    # Optional S3 upload
    maybe_upload_s3([ts_csv, latest_csv], s3_bucket)

if __name__ == "__main__":
    main()
