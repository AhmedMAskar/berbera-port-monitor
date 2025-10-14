# app_streamlit/app.py
# ------------------------------------------------------------
# Berbera Port Monitor — TEU Utilization (tug-free)
# ------------------------------------------------------------
# - Reads S3 CSVs written by scripts/s3_pipeline_shipfinder_berbera.py
# - Excludes tugs globally
# - Computes TEU or TEU-equivalent from GT/DWT/Size if missing
# - KPIs: instantaneous in-port TEU, weekly/monthly utilization vs 500k TEU/year
# - Historical in-port snapshots
# - Public vs Subscriber methodology
# ------------------------------------------------------------

import os
import io
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st
import plotly.express as px
import boto3

# =========================
# Page config & constants
# =========================
st.set_page_config(page_title="Berbera Port Monitor — TEU", layout="wide")
st.title("Berbera Port Monitor — TEU Utilization")

# --- Secrets / ENV ---
S3_BUCKET   = (st.secrets.get("S3_BUCKET")   or os.getenv("S3_BUCKET")   or "").strip()
S3_PREFIX   = (st.secrets.get("S3_PREFIX")   or os.getenv("S3_PREFIX")   or "berbera").strip().strip("/")
AWS_REGION  = (st.secrets.get("AWS_REGION")  or os.getenv("AWS_REGION")  or None)

# Operator capacity: ~500,000 TEU/year (DP World figure)
ANNUAL_TEU_TARGET   = float(st.secrets.get("ANNUAL_TEU_TARGET", os.getenv("ANNUAL_TEU_TARGET", 500_000)))
MONTHLY_TEU_TARGET  = ANNUAL_TEU_TARGET / 12.0
WEEKLY_TEU_TARGET   = ANNUAL_TEU_TARGET / 52.0
DAILY_TEU_TARGET    = ANNUAL_TEU_TARGET / 365.0

AWS_ACCESS_KEY_ID     = (st.secrets.get("AWS_ACCESS_KEY_ID")     or os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = (st.secrets.get("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"))

KNOWN_STATUSES = ["in_port", "incoming", "outgoing", "expected"]

# Estimation coefficients (same as scraper)
TEU_PER_TON = float(st.secrets.get("TEU_PER_TON", os.getenv("TEU_PER_TON", 1/12)))
K_LxB       = float(st.secrets.get("K_LxB", os.getenv("K_LxB", 0.50)))
INCLUDE_PASSENGER_AS_TEU = bool(int(st.secrets.get("INCLUDE_PASSENGER_AS_TEU", os.getenv("INCLUDE_PASSENGER_AS_TEU", "0"))))

# =========================
# S3 helpers
# =========================
def s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

def _read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

@st.cache_data(ttl=0)
def _s3_head_meta(bucket: str, key: str) -> Tuple[str, str, int]:
    s3 = s3_client()
    resp = s3.head_object(Bucket=bucket, Key=key)
    etag = resp.get("ETag", "").strip('"')
    lm = resp.get("LastModified")
    lm_iso = lm.astimezone(timezone.utc).isoformat() if lm else ""
    size = resp.get("ContentLength", 0)
    return etag, lm_iso, size

@st.cache_data(ttl=600)
def list_history_keys(limit: int = 600) -> List[str]:
    base = f"{S3_PREFIX}/history/csv/"
    s3 = s3_client()
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=base):
        for it in page.get("Contents", []):
            k = it["Key"]
            if k.endswith(".csv"):
                keys.append(k)
    keys.sort()
    return keys[-limit:]

@st.cache_data(ttl=0)
def load_vf_latest_from_s3(cache_bust: str) -> pd.DataFrame:
    key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    try:
        return _read_csv_from_s3(S3_BUCKET, key)
    except Exception as e:
        st.error(f"Could not read latest snapshot from s3://{S3_BUCKET}/{key}\n\n{e}")
        return pd.DataFrame()

@st.cache_data(ttl=0)
def load_vf_history_from_s3(cache_bust: str, limit_keys: int = 600) -> pd.DataFrame:
    keys = list_history_keys(limit=limit_keys)
    if not keys:
        return pd.DataFrame()
    frames = []
    s3 = s3_client()
    for k in keys:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=k)
            df = pd.read_csv(io.BytesIO(obj["Body"].read()))
            if "scraped_at_utc" not in df.columns:
                ts_token = k.split("/")[-1].replace(".csv", "").split("_")[-1]
                try:
                    dt_obj = datetime.strptime(ts_token, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                    df["scraped_at_utc"] = dt_obj.isoformat().replace("+00:00", "Z")
                except Exception:
                    pass
            frames.append(df)
        except Exception as e:
            st.warning(f"Failed reading {k}: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# =========================
# Data prep
# =========================
def coerce_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    if "scraped_at_utc" in df.columns:
        df["scraped_at_utc"] = pd.to_datetime(df["scraped_at_utc"], errors="coerce", utc=True)
    if "eta_to_berbera_utc" in df.columns:
        df["eta_to_berbera_utc"] = pd.to_datetime(df["eta_to_berbera_utc"], errors="coerce", utc=True)
    return df

def unify_schema(df: pd.DataFrame) -> pd.DataFrame:
    needed = [
        "scraped_at_utc","name","mmsi","ship_type","status","last_port",
        "distance_nm_to_berbera","eta_to_berbera_utc","speed_kn",
        "gt","dwt","length_m","beam_m","built_year",
        "teu_capacity_actual","teu_equiv","source"
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = None
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["ship_type"] = df["ship_type"].astype(str).str.strip().str.title()
    df = coerce_timestamps(df)
    for c in ["distance_nm_to_berbera","speed_kn","gt","dwt","length_m","beam_m","built_year","teu_equiv","teu_capacity_actual"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def add_time_bins(df: pd.DataFrame) -> pd.DataFrame:
    if "scraped_at_utc" not in df.columns:
        return df
    ts = pd.to_datetime(df["scraped_at_utc"], errors="coerce", utc=True)
    df["_ts"] = ts
    df["_date"]  = ts.dt.date
    df["_week"]  = ts.dt.to_period("W").dt.start_time
    df["_month"] = ts.dt.to_period("M").dt.to_timestamp()
    df["_year"]  = ts.dt.to_period("Y").dt.to_timestamp()
    return df

def _exclude_tugs(df: pd.DataFrame) -> pd.DataFrame:
    mask_tug = df["ship_type"].astype(str).str.contains(r"\bTug\b", case=False, na=False)
    return df[~mask_tug].copy()

# TEU fallback
def teu_from_dwt_app(dwt): return float(dwt) * TEU_PER_TON if dwt and dwt > 0 else 0.0
def teu_from_gt_app(gt):   return float(gt) / 10.0 if gt and gt > 0 else 0.0
def teu_from_lxb_app(L,B):
    if not L or not B: return 0.0
    return max(50.0, min(K_LxB * float(L) * float(B), 24000.0))

def ensure_teu_equiv(df: pd.DataFrame) -> pd.DataFrame:
    if "teu_equiv" not in df.columns:
        df["teu_equiv"] = None
    mask_missing = df["teu_equiv"].isna()
    if mask_missing.any():
        vals = []
        for _, r in df.loc[mask_missing].iterrows():
            stype = (r.get("ship_type") or "").lower()
            if "tug" in stype or "sailing" in stype:
                vals.append(0.0)
            elif "container" in stype:
                cand = [
                    teu_from_dwt_app(r.get("dwt")),
                    teu_from_gt_app(r.get("gt")),
                    teu_from_lxb_app(r.get("length_m"), r.get("beam_m"))
                ]
                cand = [x for x in cand if x and x > 0]
                vals.append(sum(cand)/len(cand) if cand else 0.0)
            elif "ro-ro" in stype or "roro" in stype:
                vals.append(teu_from_gt_app(r.get("gt")))
            elif "passenger" in stype:
                vals.append(teu_from_gt_app(r.get("gt")) if INCLUDE_PASSENGER_AS_TEU else 0.0)
            else:
                vals.append(teu_from_dwt_app(r.get("dwt")))
        df.loc[mask_missing, "teu_equiv"] = vals
    return df

# =========================
# Load data
# =========================
top = st.container()
with top:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()

latest_key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
etag, last_modified_iso, size_bytes = _s3_head_meta(S3_BUCKET, latest_key)

vf_latest = load_vf_latest_from_s3(etag)
vf_hist   = load_vf_history_from_s3(etag, limit_keys=600)

df_all = pd.concat([vf_hist, vf_latest], ignore_index=True)
df_all = unify_schema(df_all).drop_duplicates(subset=["mmsi","scraped_at_utc"], keep="last")
df_all = add_time_bins(df_all)
df_all = _exclude_tugs(df_all)
df_all = ensure_teu_equiv(df_all)

# =========================
# KPIs — TEU-based (FIXED timezone)
# =========================
def kpi_values(latest_df: pd.DataFrame) -> dict:
    if latest_df.empty:
        return dict(in_port_teu=0.0, daily_pct=0.0, weekly_teu=0.0, weekly_pct=0.0, monthly_teu=0.0, monthly_pct=0.0)
    in_port_now = latest_df[latest_df["status"] == "in_port"].copy()
    in_port_teu = pd.to_numeric(in_port_now["teu_equiv"], errors="coerce").fillna(0).sum()
    daily_pct   = round(100 * in_port_teu / DAILY_TEU_TARGET, 1) if DAILY_TEU_TARGET else 0.0

    # ✅ FIX: Use aware UTC timestamp
    now = pd.Timestamp.now(tz="UTC")
    start_week  = now - pd.Timedelta(days=7)
    start_month = now - pd.DateOffset(months=1)

    def period_teu_proxy(df, start_ts, end_ts):
        dfx = df[(df["status"]=="in_port") &
                 (df["scraped_at_utc"]>=start_ts) &
                 (df["scraped_at_utc"]< end_ts)].copy()
        if dfx.empty: return 0.0
        return (dfx.groupby("mmsi")["teu_equiv"].max().fillna(0).sum())

    weekly_teu  = period_teu_proxy(df_all, start_week, now)
    monthly_teu = period_teu_proxy(df_all, start_month, now)

    weekly_pct  = round(100 * weekly_teu  / WEEKLY_TEU_TARGET, 1)
    monthly_pct = round(100 * monthly_teu / MONTHLY_TEU_TARGET, 1)

    return dict(
        in_port_teu=round(in_port_teu, 1),
        daily_pct=daily_pct,
        weekly_teu=round(weekly_teu, 1),
        weekly_pct=weekly_pct,
        monthly_teu=round(monthly_teu, 1),
        monthly_pct=monthly_pct
    )

# =========================
# Compute latest snapshot
# =========================
def compute_latest_rows() -> pd.DataFrame:
    if not vf_latest.empty:
        df = unify_schema(vf_latest.copy())
        df = _exclude_tugs(df)
        df = ensure_teu_equiv(df)
        return df
    if not df_all.empty:
        max_ts = df_all["scraped_at_utc"].max()
        return df_all[df_all["scraped_at_utc"] == max_ts]
    return pd.DataFrame()

latest_rows = compute_latest_rows()
k = kpi_values(latest_rows)

# KPIs display
c1,c2,c3,c4,c5,c6 = st.columns(6)
c1.metric("In-Port TEU (tug-free)", f"{k['in_port_teu']:.0f}")
c2.metric("Daily vs Target", f"{k['daily_pct']}%")
c3.metric("Weekly TEU", f"{k['weekly_teu']:.0f}")
c4.metric("Weekly vs Target", f"{k['weekly_pct']}%")
c5.metric("Monthly TEU", f"{k['monthly_teu']:.0f}")
c6.metric("Monthly vs Target", f"{k['monthly_pct']}%")

fresh_ts = latest_rows["scraped_at_utc"].max() if not latest_rows.empty else None
st.caption(f"Data freshness: {fresh_ts.isoformat() if fresh_ts is not None else 'n/a'}")
st.markdown("---")

# =========================
# Tables and charts (same as before)
# =========================
statuses_present = sorted(set(x for x in df_all["status"].dropna().unique() if x in KNOWN_STATUSES)) or KNOWN_STATUSES
status = st.selectbox("View", statuses_present, index=0)

types_all = sorted(df_all["ship_type"].dropna().unique())
selected_types = st.multiselect("Ship types", types_all, default=types_all)

latest_df = latest_rows.copy()
if not latest_df.empty:
    if status:
        latest_df = latest_df[latest_df["status"] == status]
    if selected_types:
        latest_df = latest_df[latest_df["ship_type"].isin(selected_types)]

cols = ["name","mmsi","ship_type","status","last_port","gt","dwt","length_m","beam_m",
        "teu_equiv","eta_to_berbera_utc","speed_kn","scraped_at_utc"]
st.subheader(f"Latest — {status} (tug-free)")
st.dataframe(latest_df[cols], use_container_width=True, hide_index=True)
st.download_button("⬇️ Download CSV", latest_df[cols].to_csv(index=False).encode("utf-8"),
                   file_name=f"{status}_latest_teu.csv", mime="text/csv")

st.markdown("---")
st.subheader("Historical In-Port Browser (tug-free)")
df_in_hist = df_all[(df_all["status"]=="in_port") & df_all["scraped_at_utc"].notna()].copy()
if df_in_hist.empty:
    st.info("No in-port history yet.")
else:
    df_in_hist["scraped_floor"] = pd.to_datetime(df_in_hist["scraped_at_utc"], utc=True, errors="coerce").dt.floor("s")
    unique_times = sorted(df_in_hist["scraped_floor"].unique())
    chosen_ts = st.selectbox("Snapshot time (UTC)", unique_times, index=len(unique_times)-1)
    snap = df_in_hist[df_in_hist["scraped_floor"] == chosen_ts]
    st.caption(f"In port @ {chosen_ts} — {snap['mmsi'].nunique()} vessels | {snap['teu_equiv'].sum():,.0f} TEU-equiv")
    st.dataframe(snap[cols], use_container_width=True, hide_index=True)

# =========================
# Methodology
# =========================
with st.expander("ℹ️ Methodology"):
    st.markdown(f"""
**Public summary:**  
TEU and TEU-equivalents are estimated from basic vessel parameters (Gross Tonnage, Deadweight, and Length/Beam).  
For research and transparency, we normalize everything to TEU for a comparable view across ship types.

**Subscriber detail:**  
- Container ships: TEU = avg(DWT×(1/12), GT/10, k×L×B) where k = {K_LxB}.  
- Bulk/general/tankers: TEU-equiv = DWT×(1/12).  
- Ro-Ro: TEU-equiv ≈ GT/10.  
- Passenger: excluded by default.  
- Tugs excluded globally.  
- Targets = 500 000 TEU/year split by day/week/month for utilization %.  
All coefficients configurable via secrets/env for operator calibration.
""")
