# scripts/vf_scrape.py
"""
VesselFinder scraper runner (schedule controlled by GitHub Actions).

Outputs:
- data/vf_snapshot.csv                           (rolling latest)
- data/vf_snapshots/vf_snapshot_<TS>.csv         (history)
Uploads to S3 (if S3_BUCKET set):
- s3://<bucket>/<prefix>/latest/vf_snapshot.csv
- s3://<bucket>/<prefix>/history/csv/YYYY/MM/DD/HHmm/vf_snapshot_<TS>.csv

Env:
- S3_BUCKET  (required for S3 upload)
- S3_PREFIX  (optional; default: "berbera")
- AWS_REGION (optional; boto3 will still work if omitted)
"""

import os
import csv
import time
import datetime as dt
from pathlib import Path

try:
    import pandas as pd
except Exception:
    pd = None

# ------------------------------
# Demo stub: replace with real scrape
# ------------------------------
def scrape_vesselfinder():
    """
    TODO: Replace this stub with your real scraping logic.
    Must return a list[dict] or pandas.DataFrame with consistent columns.
    """
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows = [
        {
            "scraped_at_utc": now,
            "name": "Demo Vessel",
            "mmsi": "123456789",
            "ship_type": "General Cargo",
            "status": "in_port",  # or incoming/outgoing/expected
            "last_port": "Aden",
            "distance_nm_to_berbera": 0.3,
            "eta_to_berbera_utc": None,
            "speed_kn": 0.0,
            "source": "VesselFinder",
        }
    ]
    return rows

# ------------------------------
# Helpers
# ------------------------------
def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def to_dataframe(data):
    if pd is not None:
        return pd.DataFrame(data)
    return data

def write_outputs(df_or_rows, out_dir: Path) -> tuple[Path, Path]:
    _ensure_dir(out_dir)
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    ts_csv = out_dir / f"vf_snapshot_{ts}.csv"
    latest_csv = Path("data") / "vf_snapshot.csv"  # <-- rolling latest at data root

    if pd is not None and isinstance(df_or_rows, pd.DataFrame):
        df_or_rows.to_csv(ts_csv, index=False)
        df_or_rows.to_csv(latest_csv, index=False)
    else:
        rows = df_or_rows or []
        if rows:
            headers = sorted(rows[0].keys())
        else:
            headers = ["scraped_at_utc","name","mmsi","ship_type","status","last_port","distance_nm_to_berbera","eta_to_berbera_utc","speed_kn","source"]
        with open(ts_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            if rows:
                w.writerows(rows)
        latest_csv.write_text(ts_csv.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"📝 Wrote {ts_csv}")
    print(f"📝 Wrote {latest_csv}")
    return ts_csv, latest_csv

def s3_upload(local_file: Path, bucket: str, key: str) -> None:
    import boto3
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION") or None)
    s3.upload_file(str(local_file), bucket, key)
    print(f"✅ Uploaded: s3://{bucket}/{key}")

def maybe_upload_s3(ts_csv: Path, latest_csv: Path) -> None:
    bucket = (os.getenv("S3_BUCKET") or "").strip()
    if not bucket:
        print("ℹ️ S3_BUCKET not set; skipping S3 upload.")
        return
    prefix = (os.getenv("S3_PREFIX") or "berbera").strip().strip("/")
    # history path with folders by time
    ts = ts_csv.stem.split("_")[-1]  # e.g., 20251010T153000Z
    history_folder = dt.datetime.utcnow().strftime("%Y/%m/%d/%H%M")
    hist_key   = f"{prefix}/history/csv/{history_folder}/vf_snapshot_{ts}.csv"
    latest_key = f"{prefix}/latest/vf_snapshot.csv"

    s3_upload(ts_csv, bucket, hist_key)
    s3_upload(latest_csv, bucket, latest_key)

def main():
    print("🚀 Starting VesselFinder scrape…")
    t0 = time.time()

    data = scrape_vesselfinder()
    df = to_dataframe(data)
    print(f"✅ Scrape done in {time.time()-t0:.2f}s; rows={len(df) if hasattr(df,'__len__') else 'n/a'}")

    out_dir = Path("data") / "vf_snapshots"
    ts_csv, latest_csv = write_outputs(df, out_dir)
    maybe_upload_s3(ts_csv, latest_csv)

if __name__ == "__main__":
    main()
