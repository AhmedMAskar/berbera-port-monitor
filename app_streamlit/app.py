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
import boto3

# =========================
# Page config & constants
# =========================
st.set_page_config(page_title="Berbera Port Monitor", layout="wide")
st.title("Berbera Port Monitor")

# Secrets (or env fallbacks)
S3_BUCKET   = (st.secrets.get("S3_BUCKET")   or os.getenv("S3_BUCKET")   or "").strip()
S3_PREFIX   = (st.secrets.get("S3_PREFIX")   or os.getenv("S3_PREFIX")   or "berbera").strip().strip("/")
AWS_REGION  = (st.secrets.get("AWS_REGION")  or os.getenv("AWS_REGION")  or None)
CAPACITY    = int(st.secrets.get("IN_PORT_CAPACITY", os.getenv("IN_PORT_CAPACITY", 10)))

# Read-only AWS creds for the app (Option A)
AWS_ACCESS_KEY_ID     = (st.secrets.get("AWS_ACCESS_KEY_ID")     or os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = (st.secrets.get("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"))

KNOWN_STATUSES = ["in_port", "incoming", "outgoing", "expected"]


# =========================
# S3 helpers + cache-buster
# =========================
def s3_client():
    if not S3_BUCKET:
        st.error("S3_BUCKET is not configured in Streamlit secrets.")
        st.stop()
    if not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        st.error("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set in Streamlit secrets.")
        st.stop()
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
def _s3_head_etag(bucket: str, key: str) -> str:
    """ETag changes whenever 'latest' is overwritten → busts cache deterministically."""
    s3 = s3_client()
    resp = s3.head_object(Bucket=bucket, Key=key)
    return resp.get("ETag", "").strip('"')

@st.cache_data(ttl=0)
def load_vf_latest_from_s3(cache_bust: str) -> pd.DataFrame:
    key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
    try:
        return _read_csv_from_s3(S3_BUCKET, key)
    except Exception as e:
        st.error(f"Could not read latest snapshot from s3://{S3_BUCKET}/{key}\n\n{e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def list_history_keys(limit: int = 500) -> List[str]:
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
    return keys[-limit:]  # newest last

@st.cache_data(ttl=0)
def load_vf_history_from_s3(cache_bust: str, limit_keys: int = 500) -> pd.DataFrame:
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
# Data prep / metrics
# =========================
def coerce_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    if "scraped_at_utc" in df.columns:
        df["scraped_at_utc"] = pd.to_datetime(df["scraped_at_utc"], errors="coerce", utc=True)
    if "eta_to_berbera_utc" in df.columns:
        df["eta_to_berbera_utc"] = pd.to_datetime(df["eta_to_berbera_utc"], errors="coerce", utc=True)
    return df

def unify_schema(df: pd.DataFrame) -> pd.DataFrame:
    needed = [
        "scraped_at_utc", "name", "mmsi", "ship_type", "status",
        "last_port", "distance_nm_to_berbera", "eta_to_berbera_utc", "speed_kn", "source",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = None
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["ship_type"] = df["ship_type"].astype(str).str.strip().str.title()
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
    max_ts = latest_timestamp(df)
    if max_ts is None:
        return {"in_port_now": 0, "capacity": CAPACITY, "at_capacity": False, "utilization_pct": 0.0}
    latest = df[df["scraped_at_utc"] == max_ts]
    in_port_now = latest[latest["status"] == "in_port"]["mmsi"].nunique()
    pct = round(100 * in_port_now / CAPACITY, 1) if CAPACITY else 0.0
    return {"in_port_now": in_port_now, "capacity": CAPACITY, "at_capacity": in_port_now >= CAPACITY, "utilization_pct": pct}

def group_counts(df: pd.DataFrame, status: str, freq: str, ship_types: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    dfx = df.copy()
    if status:
        dfx = dfx[dfx["status"] == status]
    if ship_types:
        dfx = dfx[dfx["ship_type"].isin(ship_types)]
    if dfx.empty:
        return dfx
    dfx = dfx.dropna(subset=["scraped_at_utc"]).set_index("scraped_at_utc")
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

# Bust cache using the 'latest' object's ETag (changes on every upload)
latest_key = f"{S3_PREFIX}/latest/vf_snapshot.csv"
etag = _s3_head_etag(S3_BUCKET, latest_key)

# Load latest + history with cache-buster
vf_latest = load_vf_latest_from_s3(etag)
vf_hist   = load_vf_history_from_s3(etag, limit_keys=600)

# Combine, normalize, and dedupe
df_all = pd.concat([vf_hist, vf_latest], ignore_index=True) if not vf_latest.empty else vf_hist
df_all = unify_schema(df_all).drop_duplicates(subset=["mmsi", "scraped_at_utc"], keep="last")
df_all = add_time_bins(df_all)

# ------- Debug: show what we actually loaded (remove later if you want) -------
with st.expander("🔧 Debug – source & counts"):
    st.write("S3:", S3_BUCKET, "/", S3_PREFIX)
    st.write("Latest ETag:", etag)
    st.write("Rows in latest:", 0 if vf_latest is None else len(vf_latest))
    if not vf_latest.empty:
        st.write(vf_latest.head(5))
    st.write("Statuses in all data:", df_all["status"].value_counts(dropna=False))
# ------------------------------------------------------------------------------

# Data freshness
fresh = latest_timestamp(df_all)
st.caption(f"Data freshness (latest VF snapshot): {fresh.isoformat() if fresh else 'n/a'}")

# =========================
# KPI row (from latest snapshot)
# =========================
k1, k2, k3, k4, k5 = st.columns(5)
cap = capacity_stat(df_all)
k1.metric("In port (VF)", cap["in_port_now"])
k2.metric("Capacity", cap["capacity"])
k3.metric("Utilization", f"{cap['utilization_pct']}%")
if fresh:
    latest_rows = df_all[df_all["scraped_at_utc"] == fresh]
    k4.metric("Expected (VF)", int((latest_rows["status"] == "expected").sum()))
    k5.metric("Incoming (VF)", int((latest_rows["status"] == "incoming").sum()))
else:
    k4.metric("Expected (VF)", 0); k5.metric("Incoming (VF)", 0)

st.success("Port is below capacity.") if not cap["at_capacity"] else st.warning("Port is at or above capacity.")
st.markdown("---")

# =========================
# Controls
# =========================
c1, c2, c3, c4 = st.columns([1, 1, 2, 2])
with c1:
    freq_label = st.selectbox("Aggregation", ["Daily","Weekly","Monthly","Yearly"], index=0)
    FREQ_MAP = {"Daily":"D","Weekly":"W","Monthly":"M","Yearly":"Y"}
    freq = FREQ_MAP[freq_label]
with c2:
    statuses_present = sorted(set(x for x in df_all["status"].dropna().unique() if x in KNOWN_STATUSES)) or KNOWN_STATUSES
    status = st.selectbox("View", statuses_present, index=0)
with c3:
    all_types = sorted([t for t in df_all["ship_type"].dropna().unique().tolist() if t and t.lower() != "none"])
    selected_types = st.multiselect("Ship types", all_types, default=all_types)
with c4:
    st.caption("Download current table")

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
    latest_df = df_all[(df_all["scraped_at_utc"] == fresh) & (df_all["status"] == status)]
    if selected_types:
        latest_df = latest_df[latest_df["ship_type"].isin(selected_types)]

cols = ["name","mmsi","ship_type","status","last_port","distance_nm_to_berbera","eta_to_berbera_utc","speed_kn","scraped_at_utc","source"]
if latest_df.empty:
    st.info("No rows for the current filter yet.")
else:
    st.dataframe(latest_df[cols], use_container_width=True, hide_index=True)
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
    fig = px.area(grouped, x="ts", y="count", color="ship_type",
                  labels={"ts":"Time","count":"Distinct vessels"}, title=None)
    fig.update_layout(legend_title_text="Ship type", hovermode="x unified", margin=dict(l=0,r=0,t=10,b=0))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# =========================
# Optional: AIS / DB section
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
