# app_streamlit/app.py
# ------------------------------------------------------------
# Berbera Port Monitor — TEU (Calls · Enrichment · True/Proxy Map)
# ------------------------------------------------------------
# Map styles:
# - Incoming (Arrivals): solid green ▶▶▶
# - Expected: dashed orange ▶▶▶
# - Outgoing: solid red ◀◀◀
# - In-Port: solid purple
# Optional: heat map of historical origin ports for in-port snapshots
# ------------------------------------------------------------

import os
import io
import re
import math
from datetime import datetime, timezone
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st
import plotly.express as px
import boto3
import folium
from folium import FeatureGroup
from folium.plugins import MiniMap, PolyLineTextPath, HeatMap

# =========================
# Page config & constants
# =========================
st.set_page_config(page_title="Berbera Port Monitor — TEU", layout="wide")
st.title("Berbera Port Monitor — TEU (Port-Calls · Enrichment · Map)")

# --- Secrets / ENV ---
S3_BUCKET   = (st.secrets.get("S3_BUCKET")   or os.getenv("S3_BUCKET")   or "").strip()
S3_PREFIX   = (st.secrets.get("S3_PREFIX")   or os.getenv("S3_PREFIX")   or "berbera").strip().strip("/")
AWS_REGION  = (st.secrets.get("AWS_REGION")  or os.getenv("AWS_REGION")  or None)

# Capacity targets
ANNUAL_TEU_TARGET   = float(st.secrets.get("ANNUAL_TEU_TARGET", os.getenv("ANNUAL_TEU_TARGET", 500_000)))
MONTHLY_TEU_TARGET  = ANNUAL_TEU_TARGET / 12.0
WEEKLY_TEU_TARGET   = ANNUAL_TEU_TARGET / 52.0
DAILY_TEU_TARGET    = ANNUAL_TEU_TARGET / 365.0

# SLA (hours)
SLA_HOURS = float(st.secrets.get("SLA_HOURS", os.getenv("SLA_HOURS", 24)))

# TEU estimation coeffs (align with scraper)
TEU_PER_TON = float(st.secrets.get("TEU_PER_TON", os.getenv("TEU_PER_TON", 1/12)))
K_LxB       = float(st.secrets.get("K_LxB", os.getenv("K_LxB", 0.50)))
INCLUDE_PASSENGER_AS_TEU = bool(int(st.secrets.get("INCLUDE_PASSENGER_AS_TEU", os.getenv("INCLUDE_PASSENGER_AS_TEU", "0"))))

KNOWN_STATUSES = ["in_port", "incoming", "outgoing", "expected"]

AWS_ACCESS_KEY_ID     = (st.secrets.get("AWS_ACCESS_KEY_ID")     or os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = (st.secrets.get("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"))

# --- Berbera Somaliland approx (pier centroid)
BERBERA_LAT = float(st.secrets.get("BERBERA_LAT", os.getenv("BERBERA_LAT", "10.4396")))
BERBERA_LON = float(st.secrets.get("BERBERA_LON", os.getenv("BERBERA_LON", "45.0143")))

# Port dictionary
PORT_COORDS = {
    "aden": (12.7855, 45.0187),
    "hodeidah": (14.8020, 42.9510),
    "al hudaydah": (14.802, 42.951),
    "jebel ali": (25.0156, 55.0616),
    "dubai": (25.271, 55.308),
    "sharjah": (25.358, 55.391),
    "fujairah": (25.128, 56.334),
    "salalah": (16.9526, 54.0096),
    "muscat": (23.630, 58.551),
    "jeddah": (21.4858, 39.1925),
    "djibouti": (11.6047, 43.1430),
    "massawa": (15.608, 39.453),
    "port sudan": (19.615, 37.216),
    "bosaso": (11.282, 49.18),
    "berbera": (BERBERA_LAT, BERBERA_LON),
    "karachi": (24.842, 66.968),
    "mumbai": (18.94, 72.84),
    "chattogram": (22.249, 91.817),
    "chittagong": (22.249, 91.817),
    "mombasa": (-4.063, 39.675),
}

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
def _s3_head_meta(bucket: str, key: str):
    s3 = s3_client()
    resp = s3.head_object(Bucket=bucket, Key=key)
    etag = resp.get("ETag", "").strip('"')
    lm = resp.get("LastModified")
    lm_iso = lm.astimezone(timezone.utc).isoformat() if lm else ""
    size = resp.get("ContentLength", 0)
    return etag, lm_iso, size

@st.cache_data(ttl=600)
def list_history_keys(limit: int = 800) -> List[str]:
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
def load_vf_history_from_s3(cache_bust: str, limit_keys: int = 800) -> pd.DataFrame:
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
def fix_berbera(txt: Optional[str]) -> Optional[str]:
    if not isinstance(txt, str) or not txt.strip():
        return txt
    out = re.sub(r"\bberbera\s*,?\s*somalia\b", "Berbera, Somaliland", txt, flags=re.IGNORECASE)
    out = re.sub(r"\bberbera\b", "Berbera", out, flags=re.IGNORECASE)
    return out

def coerce_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["scraped_at_utc", "eta_to_berbera_utc", "atd_last_port_utc"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    return df

def unify_schema(df: pd.DataFrame) -> pd.DataFrame:
    needed = [
        "scraped_at_utc","name","mmsi","ship_type","status","last_port",
        "distance_nm_to_berbera","eta_to_berbera_utc","speed_kn",
        "gt","dwt","length_m","beam_m","built_year",
        "teu_capacity_actual","teu_equiv","source",
        "detail_url","destination","last_port_detailed","atd_last_port_utc",
        "course_deg","heading_deg","nav_status","direction_cardinal",
        "position_age_min","flag","imo","callsign","draught_m",
        "lat_deg","lon_deg",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = None
    df["status"]    = df["status"].astype(str).str.strip().str.lower()
    df["ship_type"] = df["ship_type"].astype(str).str.strip().str.title()

    # normalize naming
    for c in ["destination", "last_port", "last_port_detailed"]:
        if c in df.columns:
            df[c] = df[c].map(fix_berbera)

    df = coerce_timestamps(df)
    num_cols = ["distance_nm_to_berbera","speed_kn","gt","dwt","length_m","beam_m","built_year",
                "teu_equiv","teu_capacity_actual","course_deg","heading_deg","position_age_min",
                "draught_m","lat_deg","lon_deg"]
    for c in num_cols:
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
                cand = [teu_from_dwt_app(r.get("dwt")), teu_from_gt_app(r.get("gt")), teu_from_lxb_app(r.get("length_m"), r.get("beam_m"))]
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

def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "route_last_port" not in df.columns:
        df["route_last_port"] = df.get("last_port_detailed")
        df["route_last_port"] = df["route_last_port"].fillna(df.get("last_port"))
    df["route_last_port"] = df["route_last_port"].map(fix_berbera)
    # fill missing direction cardinal from course
    if "direction_cardinal" in df.columns and "course_deg" in df.columns:
        card = df["direction_cardinal"].astype(object)
        missing = card.isna() | (card.astype(str) == "") | (card.astype(str).str.lower() == "none")
        def to_card(deg):
            if pd.isna(deg): return None
            dirs = ["N","NE","E","SE","S","SW","W","NW"]
            return dirs[int(((float(deg) % 360) + 22.5)//45) % 8]
        card.loc[missing] = df.loc[missing, "course_deg"].map(to_card)
        df["direction_cardinal"] = card
    return df

def cols_present(df: pd.DataFrame, desired: List[str]) -> List[str]:
    return [c for c in desired if c in df.columns]

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
vf_hist   = load_vf_history_from_s3(etag, limit_keys=800)

df_all = pd.concat([vf_hist, vf_latest], ignore_index=True)
df_all = unify_schema(df_all).drop_duplicates(subset=["mmsi","scraped_at_utc"], keep="last")
df_all = add_time_bins(df_all)
df_all = _exclude_tugs(df_all)
df_all = ensure_teu_equiv(df_all)
df_all = add_derived_fields(df_all)

# =========================
# Port-call detection + SLA
# =========================
@st.cache_data(ttl=0)
def build_port_calls(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        cols = ["mmsi","name","ship_type","arrived_at","departed_at","dwell_hours","call_teu","last_port","last_port_detailed"]
        return pd.DataFrame(columns=cols)
    dfx = df.sort_values(["mmsi","scraped_at_utc"]).reset_index(drop=True)
    rows = []
    now = pd.Timestamp.now(tz="UTC")
    for mmsi, g in dfx.groupby("mmsi", sort=False):
        g = g.sort_values("scraped_at_utc")
        name = g["name"].dropna().iloc[-1] if not g["name"].dropna().empty else None
        ship_type = g["ship_type"].dropna().iloc[-1] if not g["ship_type"].dropna().empty else None
        in_call = False; call_start = None; call_teus = []; lps = []; lpds = []
        for _, r in g.iterrows():
            ts = r["scraped_at_utc"]; status = (r["status"] or "").lower()
            teu = float(r.get("teu_equiv") or 0.0); lp  = r.get("last_port"); lpd = r.get("last_port_detailed")
            if (not in_call) and status == "in_port":
                in_call, call_start = True, ts
                if pd.notna(teu): call_teus=[teu]
                if pd.notna(lp):  lps=[lp]
                if pd.notna(lpd): lpds=[lpd]
                continue
            if in_call and status == "in_port":
                if pd.notna(teu): call_teus.append(teu)
                if pd.notna(lp):  lps.append(lp)
                if pd.notna(lpd): lpds.append(lpd)
                continue
            if in_call and status != "in_port":
                departed_at = ts
                call_teu = max(call_teus) if call_teus else 0.0
                lp_val  = pd.Series(lps).mode().iloc[0]  if lps  else None
                lpd_val = pd.Series(lpds).mode().iloc[0] if lpds else None
                dwell_h = (departed_at - call_start).total_seconds()/3600.0
                rows.append(dict(mmsi=mmsi,name=name,ship_type=ship_type,arrived_at=call_start,departed_at=departed_at,
                                 dwell_hours=dwell_h,call_teu=float(call_teu),last_port=lp_val,last_port_detailed=lpd_val))
                in_call=False; call_start=None; call_teus=[]; lps=[]; lpds=[]
        if in_call and call_start is not None:
            call_teu = max(call_teus) if call_teus else 0.0
            lp_val  = pd.Series(lps).mode().iloc[0]  if lps  else None
            lpd_val = pd.Series(lpds).mode().iloc[0] if lpds else None
            dwell_h = (now - call_start).total_seconds()/3600.0
            rows.append(dict(mmsi=mmsi,name=name,ship_type=ship_type,arrived_at=call_start,departed_at=None,
                             dwell_hours=dwell_h,call_teu=float(call_teu),last_port=lp_val,last_port_detailed=lpd_val))
    calls = pd.DataFrame(rows)
    if calls.empty: return calls
    calls["arrived_at"]  = pd.to_datetime(calls["arrived_at"],  utc=True)
    calls["departed_at"] = pd.to_datetime(calls["departed_at"], utc=True)
    calls["completed"]   = calls["departed_at"].notna()
    calls["dwell_hours"] = pd.to_numeric(calls["dwell_hours"], errors="coerce")
    calls["call_teu"]    = pd.to_numeric(calls["call_teu"], errors="coerce").fillna(0.0)
    calls = calls.sort_values(["mmsi","arrived_at"]).reset_index(drop=True)
    calls["call_seq"] = calls.groupby("mmsi").cumcount() + 1
    return calls

def with_sla_flags(calls: pd.DataFrame) -> pd.DataFrame:
    if calls.empty: return calls
    dfc = calls.copy()
    dfc["sla_hours"] = SLA_HOURS
    dfc["sla_breach"] = pd.to_numeric(dfc["dwell_hours"], errors="coerce") > dfc["sla_hours"]
    dfc["sla_label"] = dfc["sla_breach"].map(lambda b: "⚠️ Breach" if b else "✅ On track")
    return dfc

port_calls = with_sla_flags(build_port_calls(df_all))

# =========================
# KPIs
# =========================
def compute_instant_inport_teu(latest_df: pd.DataFrame) -> float:
    if latest_df.empty: return 0.0
    in_port_now = latest_df[latest_df["status"] == "in_port"]
    return pd.to_numeric(in_port_now["teu_equiv"], errors="coerce").fillna(0).sum()

def call_based_period_teu(calls_df: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> float:
    if calls_df.empty: return 0.0
    dfx = calls_df[(calls_df["arrived_at"] >= start_ts) & (calls_df["arrived_at"] < end_ts)]
    return float(dfx["call_teu"].sum()) if not dfx.empty else 0.0

def kpi_values(latest_df: pd.DataFrame, calls_df: pd.DataFrame) -> dict:
    now = pd.Timestamp.now(tz="UTC")
    start_week  = now - pd.Timedelta(days=7)
    start_month = now - pd.DateOffset(months=1)
    in_port_teu = round(compute_instant_inport_teu(latest_df), 1)
    daily_pct   = round(100 * in_port_teu / DAILY_TEU_TARGET, 1) if DAILY_TEU_TARGET else 0.0
    weekly_teu  = round(call_based_period_teu(calls_df, start_week, now), 1)
    monthly_teu = round(call_based_period_teu(calls_df, start_month, now), 1)
    weekly_pct  = round(100 * weekly_teu  / WEEKLY_TEU_TARGET, 1) if WEEKLY_TEU_TARGET else 0.0
    monthly_pct = round(100 * monthly_teu / MONTHLY_TEU_TARGET, 1) if MONTHLY_TEU_TARGET else 0.0
    return dict(in_port_teu=in_port_teu,daily_pct=daily_pct,weekly_teu=weekly_teu,weekly_pct=weekly_pct,
                monthly_teu=monthly_teu,monthly_pct=monthly_pct)

def compute_latest_rows() -> pd.DataFrame:
    if not vf_latest.empty:
        df = unify_schema(vf_latest.copy()); df = _exclude_tugs(df); df = ensure_teu_equiv(df); df = add_derived_fields(df); return df
    if not df_all.empty:
        max_ts = df_all["scraped_at_utc"].max(); return add_derived_fields(df_all[df_all["scraped_at_utc"] == max_ts])
    return pd.DataFrame()

latest_rows = compute_latest_rows()
k = kpi_values(latest_rows, port_calls)

# KPIs view
c1,c2,c3,c4,c5,c6 = st.columns(6)
c1.metric("In-Port TEU (tug-free)", f"{k['in_port_teu']:.0f}")
c2.metric("Daily vs Target", f"{k['daily_pct']}%")
c3.metric("Weekly TEU (arrivals)", f"{k['weekly_teu']:.0f}")
c4.metric("Weekly vs Target", f"{k['weekly_pct']}%")
c5.metric("Monthly TEU (arrivals)", f"{k['monthly_teu']:.0f}")
c6.metric("Monthly vs Target", f"{k['monthly_pct']}%")

fresh_ts = latest_rows["scraped_at_utc"].max() if not latest_rows.empty else None
st.caption(f"Data freshness: {fresh_ts.isoformat() if fresh_ts is not None else 'n/a'}")

st.markdown(" ")

# =========================
# TEU Trend
# =========================
c_left, c_right = st.columns([1, 1.25])

with c_right:
    st.subheader("TEU Trend — Choose Counting Mode")
    mode = st.radio("Counting Mode", ["Arrivals (no double count)", "In-Port Snapshot"], horizontal=True)

    type_opts_all = sorted(df_all["ship_type"].dropna().unique())
    group_mode = st.radio("Group by", ["Daily", "Weekly"], horizontal=True)

    if mode == "Arrivals (no double count)":
        if port_calls.empty:
            st.info("No detected arrivals yet. Trend will appear once port-calls are detected.")
        else:
            pc = port_calls.copy()
            pc["ship_type"] = pc["ship_type"].astype(str).str.strip().str.title()
            pc["arrived_at"] = pd.to_datetime(pc["arrived_at"], errors="coerce", utc=True)
            pc = pc.dropna(subset=["arrived_at"])
            pc["arrive_date"] = pc["arrived_at"].dt.date
            pc["arrive_week"] = pc["arrived_at"].dt.to_period("W").dt.start_time.dt.date
            pc["call_teu"] = pd.to_numeric(pc["call_teu"], errors="coerce").fillna(0.0)

            type_opts = sorted([t for t in pc["ship_type"].dropna().unique()]) or type_opts_all
            sel_types = st.multiselect("Ship type(s)", type_opts, default=type_opts)

            if pc["arrive_date"].empty:
                st.info("No arrival timestamps to chart.")
            else:
                min_d = pc["arrive_date"].min(); max_d = pc["arrive_date"].max()
                d1, d2 = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)

                f = pc.copy()
                if sel_types:
                    f = f[f["ship_type"].isin(sel_types)]
                f = f[(f["arrive_date"] >= d1) & (f["arrive_date"] <= d2)]

                if group_mode == "Daily":
                    g = f.groupby("arrive_date", as_index=False)["call_teu"].sum() \
                         .rename(columns={"arrive_date":"period","call_teu":"teu"})
                    title = "TEU by Day — Arrivals (counted once)"
                else:
                    g = f.groupby("arrive_week", as_index=False)["call_teu"].sum() \
                         .rename(columns={"arrive_week":"period","call_teu":"teu"})
                    title = "TEU by Week — Arrivals (counted once)"

                if not g.empty:
                    fig = px.bar(g, x="period", y="teu", title=title)
                    st.plotly_chart(fig, use_container_width=True)
                    st.download_button(
                        "⬇️ Download TEU trend CSV",
                        g.to_csv(index=False).encode("utf-8"),
                        file_name=f"teu_trend_arrivals_{'daily' if group_mode=='Daily' else 'weekly'}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No data in the selected filters/date range.")
    else:
        snap = df_all[df_all["status"]=="in_port"].copy()
        if snap.empty:
            st.info("No in-port snapshots yet.")
        else:
            snap["scraped_at_utc"] = pd.to_datetime(snap["scraped_at_utc"], errors="coerce", utc=True)
            snap = snap.dropna(subset=["scraped_at_utc"])
            snap["ship_type"] = snap["ship_type"].astype(str).str.strip().str.title()
            snap["teu_equiv"] = pd.to_numeric(snap["teu_equiv"], errors="coerce").fillna(0.0)
            snap["date"] = snap["scraped_at_utc"].dt.date
            snap["week"] = snap["scraped_at_utc"].dt.to_period("W").dt.start_time.dt.date

            type_opts = sorted([t for t in snap["ship_type"].dropna().unique()]) or type_opts_all
            sel_types = st.multiselect("Ship type(s)", type_opts, default=type_opts)

            if snap["date"].empty:
                st.info("No timestamps to chart.")
            else:
                min_d = snap["date"].min(); max_d = snap["date"].max()
                d1, d2 = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d, key="snap_dates")

                f = snap.copy()
                if sel_types:
                    f = f[f["ship_type"].isin(sel_types)]
                f = f[(f["date"] >= d1) & (f["date"] <= d2)]

                if group_mode == "Daily":
                    tmp = f.groupby(["date","mmsi"], as_index=False)["teu_equiv"].max()
                    g = tmp.groupby("date", as_index=False)["teu_equiv"].sum().rename(columns={"date":"period","teu_equiv":"teu"})
                    title = "TEU by Day — In-Port Snapshot"
                else:
                    tmp = f.groupby(["week","mmsi"], as_index=False)["teu_equiv"].max()
                    g = tmp.groupby("week", as_index=False)["teu_equiv"].sum().rename(columns={"week":"period","teu_equiv":"teu"})
                    title = "TEU by Week — In-Port Snapshot"

                if not g.empty:
                    fig = px.bar(g, x="period", y="teu", title=title)
