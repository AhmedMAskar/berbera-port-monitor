# app_streamlit/app.py
# ------------------------------------------------------------
# Berbera Port Monitor — S3-backed VesselFinder snapshot app
# ------------------------------------------------------------
# - Reads "latest" + "history" VF CSVs from S3
# - Tables for In-Port / Incoming / Outgoing / Expected
# - Charts: Daily/Weekly/Monthly/Yearly, stacked by ship type
# - Capacity stat (in-port vs capacity)
# - Optional AIS/DB section (only if DATABASE_URL provided)
# ------------------------------------------------------------

import os
import io
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd
import streamlit as st
import plotly.express as px

# We import boto3 only when needed so the app still loads if not installed locally
import boto3


# =========================
# Page config & constants
# =========================
st.set_page_config(page_title="Berbera Port Monitor", layout="wide")
st.title("Berbera Port Monitor")

# Optional: tweak these defaults via Streamlit secrets
S3_BUCKET   = (st.secrets.get("S3_BUCKET")   or os.getenv("S3_BUCKET")   or "").strip()
S3_PREFIX   = (st.secrets.get("S3_PREFIX")   or os.getenv("S3_PREFIX")   or "berbera").strip().strip("/")
AWS_REGION  = (st.secrets.get("AWS_REGION")  or os.getenv("AWS_REGION")  or None)
CAPACITY    = int(st.secrets.get("IN_PORT_CAPACITY", os.getenv("IN_PORT_CAPACITY", 10)))

# Status labels we’ll recognize for filtering/UI
KNOWN_STATUSES = ["in_port", "incoming", "outgoing", "expected"]


# =========================
# S3 helpers (cached)
# =========================
def s3_client():
    # If region is None, boto3 will still work (falls back to env/config)
    return boto3.client("s3", region_name=AWS_REGION)

def _read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

@st.cache_data(ttl=60)
def load_vf_latest_from_s3() -> pd.DataFrame:
    if not S3_BUCKET:
        st.error("S3_BUCKET is not configured. Set it in Streamlit secrets or environment.")
        st.stop()
    key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    try:
        df = _read_csv_from_s3(S3_BUCKET, key)
    except Exception as e:
        st.error(f"Could not read latest snapshot from s3://{S3_BUCKET}/{key}\n\n{e}")
        return pd.DataFrame()
    return df

@st.cache_data(ttl=600)
def list_history_keys(limit: int = 500) -> List[str]:
    """List history CSV keys under prefix/history/csv/… (newest last)"""
    if not S3_BUCKET:
        return []
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
    return keys[-limit:]  # last N

@st.cache_data(ttl=600)
def load_vf_history_from_s3(limit_keys: int = 500) -> pd.DataFrame:
    keys = list_history_keys(limit=limit_keys)
    if not keys:
        return pd.DataFrame()
    frames = []
    s3 = s3_client()
    for k in keys:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=k)
            df = pd.read_csv(io.BytesIO(obj["Body"].read()))
            # If no scrape timestamp column, derive from filename
            if "scraped_at_utc" not in df.columns:
                # Expecting vf_snapshot_YYYYMMDDTHHMMSSZ.csv
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
# Data prep / metrics
# =========================
def coerce_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    if "scraped_at_utc" in df.columns:
        df["scraped_at_utc"] = pd.to_datetime(df["scraped_at_utc"], errors="coerce", utc=True)
    if "eta_to_berbera_utc" in df.columns:
        df["eta_to_berbera_utc"] = pd.to_datetime(df["eta_to_berbera_utc"], errors="coerce", utc=True)
    return df

def unify_schema(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure expected columns exist; fill if missing so the UI never breaks
    needed = [
        "scraped_at_utc", "name", "mmsi", "ship_type", "status",
        "last_port", "distance_nm_to_berbera", "eta_to_berbera_utc", "speed_kn", "source",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = None
    # Normalize values
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["ship_type"] = df["ship_type"].astype(str).str.strip().str.title()
    # Parse timestamps and numeric
    df = coerce_timestamps(df)
    if "distance_nm_to_berbera" in df.columns:
        df["distance_nm_to_berbera"] = pd.to_numeric(df["distance_nm_to_berbera"], errors="coerce")
    if "speed_kn" in df.columns:
        df["speed_kn"] = pd.to_numeric(df["speed_kn"], errors="coerce")
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

def latest_timestamp(df: pd.DataFrame) -> Optional[datetime]:
    if "scraped_at_utc" not in df.columns or df.empty:
        return None
    return pd.to_datetime(df["scraped_at_utc"], errors="coerce", utc=True).max()

def capacity_stat(df: pd.DataFrame) -> dict:
    if df.empty or "status" not in df or "scraped_at_utc" not in df:
        return {"in_port_now": 0, "capacity": CAPACITY, "at_capacity": False, "utilization_pct": 0.0}
    # Consider the most recent snapshot time for "now"
    max_ts = latest_timestamp(df)
    if max_ts is None:
        return {"in_port_now": 0, "capacity": CAPACITY, "at_capacity": False, "utilization_pct": 0.0}
    latest = df[df["scraped_at_utc"] == max_ts]
    in_port_now = latest[latest["status"] == "in_port"]["mmsi"].nunique()
    pct = round(100 * in_port_now / CAPACITY, 1) if CAPACITY else 0.0
    return {
        "in_port_now": in_port_now,
        "capacity": CAPACITY,
        "at_capacity": in_port_now >= CAPACITY,
        "utilization_pct": pct,
    }

def group_counts(df: pd.DataFrame, status: str, freq: str, ship_types: List[str]) -> pd.DataFrame:
    """
    freq in {'D','W','M','Y'}
    Count DISTINCT vessels per period, split by ship_type.
    """
    if df.empty:
        return pd.DataFrame()
    dfx = df.copy()
    # Filter by status if present
    if status:
        dfx = dfx[dfx["status"] == status]
    # Filter ship types
    if ship_types:
        dfx = dfx[dfx["ship_type"].isin(ship_types)]
    if dfx.empty:
        return dfx

    # Use scraped_at_utc as time index
    dfx = dfx.dropna(subset=["scraped_at_utc"])
    dfx = dfx.set_index("scraped_at_utc")

    # Count DISTINCT MMSI per period per ship type
    grouped = (
        dfx.groupby("ship_type")
           .resample(freq)["mmsi"].nunique()
           .rename("count")
           .reset_index()
           .rename(columns={"scraped_at_utc": "ts"})
    )
    return grouped


# =========================
# UI: refresh & load data
# =========================
top = st.container()
with top:
    left, right = st.columns([1, 3])
    with left:
        if st.button("🔄 Refresh data", help="Clear cache and reload from S3"):
            st.cache_data.clear()
            st.rerun()

# Load latest + history
vf_latest = load_vf_latest_from_s3()
vf_hist   = load_vf_history_from_s3(limit_keys=600)  # tune how much history to pull
df_all    = pd.concat([vf_hist, vf_latest], ignore_index=True) if not vf_latest.empty else vf_hist
df_all    = unify_schema(df_all).drop_duplicates(subset=["mmsi", "scraped_at_utc"], keep="last")
df_all    = add_time_bins(df_all)

# Data freshness
fresh = latest_timestamp(df_all)
st.caption(f"Data freshness (latest VF snapshot): {fresh.isoformat() if fresh else 'n/a'}")


# =========================
# KPI row (from latest snapshot)
# =========================
kcol1, kcol2, kcol3, kcol4, kcol5 = st.columns(5)
cap = capacity_stat(df_all)
kcol1.metric("In port (VF)", cap["in_port_now"])
kcol2.metric("Capacity", cap["capacity"])
kcol3.metric("Utilization", f"{cap['utilization_pct']}%")
# From latest snapshot, compute expected/incoming/outgoing counts
if fresh:
    latest_rows = df_all[df_all["scraped_at_utc"] == fresh]
    kcol4.metric("Expected (VF)", int((latest_rows["status"] == "expected").sum()))
    kcol5.metric("Incoming (VF)", int((latest_rows["status"] == "incoming").sum()))
else:
    kcol4.metric("Expected (VF)", 0)
    kcol5.metric("Incoming (VF)", 0)

if cap["at_capacity"]:
    st.warning("Port is at or above capacity.")
else:
    st.success("Port is below capacity.")


st.markdown("---")

# =========================
# Controls
# =========================
ctrl1, ctrl2, ctrl3, ctrl4 = st.columns([1, 1, 2, 2])

with ctrl1:
    # Frequency map
    freq_label = st.selectbox("Aggregation", ["Daily", "Weekly", "Monthly", "Yearly"], index=0)
    FREQ_MAP = {"Daily": "D", "Weekly": "W", "Monthly": "M", "Yearly": "Y"}
    freq = FREQ_MAP[freq_label]

with ctrl2:
    # Only show statuses that actually exist in the data (fallback to defaults)
    statuses_present = sorted(set(x for x in df_all["status"].dropna().unique() if x in KNOWN_STATUSES))
    if not statuses_present:
        statuses_present = KNOWN_STATUSES
    status = st.selectbox("View", statuses_present, index=0)

with ctrl3:
    all_types = sorted([t for t in df_all["ship_type"].dropna().unique().tolist() if t and t.lower() != "none"])
    selected_types = st.multiselect("Ship types", all_types, default=all_types)

with ctrl4:
    st.caption("Download current table")
    # We'll fill the dataframe below, then render a download button


# =========================
# Latest table for selected view
# =========================
st.subheader({
    "in_port": "In-Port Vessels — Latest",
    "incoming": "Incoming Vessels — Latest",
    "outgoing": "Outgoing Vessels — Latest",
    "expected": "Expected Vessels — Latest",
}.get(status, "Vessel List — Latest"))

latest_df = pd.DataFrame()
if fresh:
    latest_df = df_all[(df_all["scraped_at_utc"] == fresh)]
    latest_df = latest_df[(latest_df["status"] == status)]
    if selected_types:
        latest_df = latest_df[latest_df["ship_type"].isin(selected_types)]

cols = ["name","mmsi","ship_type","status","last_port","distance_nm_to_berbera","eta_to_berbera_utc","speed_kn","scraped_at_utc","source"]
if latest_df.empty:
    st.info("No rows for the current filter yet.")
else:
    st.dataframe(latest_df[cols], use_container_width=True, hide_index=True)

    # Download button for current table
    csv_bytes = latest_df[cols].to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", data=csv_bytes, file_name=f"{status}_latest.csv", mime="text/csv")

st.markdown("---")

# =========================
# Trend chart (stacked by ship type)
# =========================
st.subheader(f"Traffic over time — {freq_label} (distinct vessels, by ship type)")
grouped = group_counts(df_all, status=status, freq=freq, ship_types=selected_types)
if grouped.empty:
    st.info("No time series yet for the selected filters.")
else:
    fig = px.area(
        grouped,
        x="ts",
        y="count",
        color="ship_type",
        title=None,
        labels={"ts": "Time", "count": "Distinct vessels"},
    )
    fig.update_layout(legend_title_text="Ship type", hovermode="x unified", margin=dict(l=0,r=0,t=10,b=0))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")


# =========================
# Optional: AIS / DB metrics (only if DATABASE_URL set)
# =========================
with st.expander("🛰️ AIS-derived metrics (optional — requires DATABASE_URL)"):
    DATABASE_URL = (
        (os.environ.get("DATABASE_URL") or "").strip().strip('"').strip("'")
        or st.secrets.get("DATABASE_URL")
    )
    if not DATABASE_URL:
        st.info("DATABASE_URL not configured — skipping AIS/DB section.")
    else:
        try:
            import psycopg2
            # Quick helper to query
            @st.cache_data(ttl=60)
            def q(sql: str, params=None) -> pd.DataFrame:
                conn = psycopg2.connect(DATABASE_URL)
                try:
                    df = pd.read_sql(sql, conn, params=params)
                finally:
                    conn.close()
                return df

            who = q("SELECT current_user, current_database();")
            st.write("Connected to DB as:", who.iloc[0].to_dict())

            exists = q("""
                SELECT COUNT(*) AS c
                FROM information_schema.tables
                WHERE table_schema='public' AND table_name='port_calls';
            """)
            if int(exists["c"].iat[0]) == 0:
                st.info("Table 'port_calls' not found yet.")
            else:
                k_alongside = q("SELECT COUNT(*) AS c FROM port_calls WHERE departure_at IS NULL;")
                st.metric("Alongside now (AIS)", int(k_alongside["c"].iat[0] or 0))
        except Exception as e:
            st.warning(f"AIS metrics skipped: {e}")
