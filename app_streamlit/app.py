# app_streamlit/app.py
# ------------------------------------------------------------
# Berbera Port Monitor — TEU (Calls · Enrichment · True/Proxy Map)
# ------------------------------------------------------------
# Map logic (always draw):
# - If lat/lon: plot ship point + segment to/from Berbera Somaliland
# - Else if Last Port / Destination matches PORT_COORDS: draw geodesic from that port
# - Else: draw bearing ray
# - Distinct styles:
#     * Incoming (Arrivals): solid green ▶▶▶ (to Berbera)
#     * Expected: dashed orange ▶▶▶ (to Berbera)
#     * Outgoing: solid red ◀◀◀ (from Berbera)
#     * In-Port: solid purple (short berth line / dot)
# - Optional: heat map of historical in-port origins (density of last ports)
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

# TEU estimation coeffs (keep aligned with scraper)
TEU_PER_TON = float(st.secrets.get("TEU_PER_TON", os.getenv("TEU_PER_TON", 1/12)))
K_LxB       = float(st.secrets.get("K_LxB", os.getenv("K_LxB", 0.50)))
INCLUDE_PASSENGER_AS_TEU = bool(int(st.secrets.get("INCLUDE_PASSENGER_AS_TEU", os.getenv("INCLUDE_PASSENGER_AS_TEU", "0"))))

KNOWN_STATUSES = ["in_port", "incoming", "outgoing", "expected"]

AWS_ACCESS_KEY_ID     = (st.secrets.get("AWS_ACCESS_KEY_ID")     or os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = (st.secrets.get("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"))

# --- Berbera Somaliland approx (pier centroid if you have it)
BERBERA_LAT = float(st.secrets.get("BERBERA_LAT", os.getenv("BERBERA_LAT", "10.4396")))
BERBERA_LON = float(st.secrets.get("BERBERA_LON", os.getenv("BERBERA_LON", "45.0143")))

# ---------- Name/Place normalizer ----------
def fix_berbera(txt: Optional[str]) -> Optional[str]:
    """Normalize 'Berbera, Somalia' → 'Berbera, Somaliland' and capitalize 'Berbera'."""
    if not isinstance(txt, str) or not txt.strip():
        return txt
    out = re.sub(r"\bberbera\s*,?\s*somalia\b", "Berbera, Somaliland", txt, flags=re.IGNORECASE)
    out = re.sub(r"\bberbera\b", "Berbera", out, flags=re.IGNORECASE)
    return out

# Common origin/destination ports near the lane-way — extend as needed
PORT_COORDS = {
    # Yemen
    "aden": (12.7855, 45.0187),
    "hodeidah": (14.8020, 42.9510),
    "al hudaydah": (14.802, 42.951),
    # UAE
    "jebel ali": (25.0156, 55.0616),
    "dubai": (25.271, 55.308),
    "sharjah": (25.358, 55.391),
    "fujairah": (25.128, 56.334),
    # Oman
    "salalah": (16.9526, 54.0096),
    "muscat": (23.630, 58.551),
    # Saudi
    "jeddah": (21.4858, 39.1925),
    # Djibouti / Eritrea / Sudan
    "djibouti": (11.6047, 43.1430),
    "massawa": (15.608, 39.453),
    "port sudan": (19.615, 37.216),
    # Somalia / Somaliland
    "bosaso": (11.282, 49.18),
    "berbera": (BERBERA_LAT, BERBERA_LON),
    # Pakistan / India / etc (useful long-haul)
    "karachi": (24.842, 66.968),
    "mumbai": (18.94, 72.84),
    "chattogram": (22.249, 91.817),
    "chittagong": (22.249, 91.817),
    # Kenya
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
def _s3_head_meta(bucket: str, key: str) -> Tuple[str, str, int]:
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
        # enrichment
        "detail_url","destination","last_port_detailed","atd_last_port_utc",
        "course_deg","heading_deg","nav_status","direction_cardinal",
        "position_age_min","flag","imo","callsign","draught_m",
        "lat_deg","lon_deg",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = None

    # normalize text fields
    for c in ["destination", "last_port", "last_port_detailed"]:
        if c in df.columns:
            df[c] = df[c].map(fix_berbera)

    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["ship_type"] = df["ship_type"].astype(str).str.strip().str.title()
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
    # route_last_port
    if "route_last_port" not in df.columns:
        df["route_last_port"] = df.get("last_port_detailed")
        df["route_last_port"] = df["route_last_port"].fillna(df.get("last_port"))
    df["route_last_port"] = df["route_last_port"].map(fix_berbera)

    # cardinal from course if missing
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
# KPIs — call-based
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

# KPIs display
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
# TEU Trend (Arrivals vs In-Port Snapshots)
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
                    st.plotly_chart(fig, use_container_width=True)
                    st.download_button(
                        "⬇️ Download TEU trend CSV",
                        g.to_csv(index=False).encode("utf-8"),
                        file_name=f"teu_trend_inport_{'daily' if group_mode=='Daily' else 'weekly'}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No data in the selected filters/date range.")

st.markdown("---")

# =========================
# Current status (enriched prefs)
# =========================
statuses_present = sorted(set(x for x in df_all["status"].dropna().unique() if x in KNOWN_STATUSES)) or KNOWN_STATUSES
status = st.selectbox("View", statuses_present, index=0)
types_all = sorted(df_all["ship_type"].dropna().unique())
selected_types = st.multiselect("Ship types", types_all, default=types_all)

def compute_latest_rows():
    if not vf_latest.empty:
        df = unify_schema(vf_latest.copy()); df = _exclude_tugs(df); df = ensure_teu_equiv(df); df = add_derived_fields(df); return df
    if not df_all.empty:
        max_ts = df_all["scraped_at_utc"].max(); return add_derived_fields(df_all[df_all["scraped_at_utc"] == max_ts])
    return pd.DataFrame()

latest_rows = compute_latest_rows()

latest_df = latest_rows.copy()
if not latest_df.empty:
    if status: latest_df = latest_df[latest_df["status"] == status]
    if selected_types: latest_df = latest_df[latest_df["ship_type"].isin(selected_types)]

cols_latest = [
    "name","mmsi","ship_type","status","route_last_port","destination","direction_cardinal",
    "gt","dwt","length_m","beam_m","teu_equiv","eta_to_berbera_utc",
    "speed_kn","course_deg","scraped_at_utc","detail_url","lat_deg","lon_deg"
]
st.subheader(f"Latest — {status} (tug-free, enriched)")
st.dataframe(latest_df[cols_present(latest_df, cols_latest)], use_container_width=True, hide_index=True)
st.download_button("⬇️ Download CSV",
                   latest_df[cols_present(latest_df, cols_latest)].to_csv(index=False).encode("utf-8"),
                   file_name=f"{status}_latest_teu.csv", mime="text/csv")

st.markdown("---")

# =========================
# Map helpers + layers
# =========================
st.subheader("Map — Berbera Somaliland & Vicinity (True/Proxy)")

def bearing_from_cardinal(card: str) -> Optional[float]:
    m = {"N":0,"NE":45,"E":90,"SE":135,"S":180,"SW":225,"W":270,"NW":315}
    return m.get(str(card).upper()) if isinstance(card, str) else None

def fwd_destination(lat, lon, bearing_deg, distance_km):
    R = 6371.0088  # km
    φ1 = math.radians(lat); λ1 = math.radians(lon); θ = math.radians(bearing_deg); d = distance_km
    φ2 = math.asin(math.sin(φ1)*math.cos(d/R) + math.cos(φ1)*math.sin(d/R)*math.cos(θ))
    λ2 = λ1 + math.atan2(math.sin(θ)*math.sin(d/R)*math.cos(φ1),
                         math.cos(d/R)-math.sin(φ1)*math.sin(φ2))
    return (math.degrees(φ2), ((math.degrees(λ2)+540)%360)-180)

def direction_ray(lat0, lon0, bearing_deg, length_km=60):
    lat1, lon1 = fwd_destination(lat0, lon0, bearing_deg, length_km)
    return [(lat0, lon0), (lat1, lon1)]

def initial_bearing(lat1, lon1, lat2, lon2):
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δλ = math.radians(lon2 - lon1)
    y = math.sin(Δλ) * math.cos(φ2)
    x = math.cos(φ1)*math.sin(φ2) - math.sin(φ1)*math.cos(φ2)*math.cos(Δλ)
    brng = (math.degrees(math.atan2(y, x)) + 360) % 360
    return brng

def port_lookup(name: Optional[str]) -> Optional[Tuple[float,float,str]]:
    if not isinstance(name, str) or not name.strip():
        return None
    key = name.strip().lower()
    if key in PORT_COORDS:
        lat,lon = PORT_COORDS[key]; return (lat,lon,name)
    key2 = key.split(",")[0].strip()
    if key2 in PORT_COORDS:
        lat,lon = PORT_COORDS[key2]; return (lat,lon,name)
    return None

incoming_df = latest_rows[(latest_rows["status"]=="incoming")].copy()
outgoing_df = latest_rows[(latest_rows["status"]=="outgoing")].copy()
expected_df = latest_rows[(latest_rows["status"]=="expected")].copy()
inport_df   = latest_rows[(latest_rows["status"]=="in_port")].copy()

def get_bearing(row):
    if "course_deg" in row and pd.notna(row["course_deg"]):
        try: return float(row["course_deg"])
        except Exception: pass
    if "direction_cardinal" in row and pd.notna(row["direction_cardinal"]):
        return bearing_from_cardinal(row["direction_cardinal"])
    return None

m = folium.Map(location=[BERBERA_LAT, BERBERA_LON], tiles="OpenStreetMap", zoom_start=9, control_scale=True)

# Port marker (Berbera Somaliland)
folium.Marker([BERBERA_LAT, BERBERA_LON],
              tooltip="Berbera Somaliland",
              popup="Berbera Somaliland",
              icon=folium.Icon(color="blue", icon="anchor", prefix="fa")).add_to(m)

# Layers
fg_in    = FeatureGroup(name="Incoming (Arrivals)", show=True)
fg_exp   = FeatureGroup(name="Expected (Scheduled)", show=True)
fg_out   = FeatureGroup(name="Outgoing", show=True)
fg_berth = FeatureGroup(name="In-Port (berth)", show=True)
fg_heat  = FeatureGroup(name="Origin Heat (historical in-port)", show=False)

STATUS_COLORS = {
    "incoming": "#2ca02c",  # green (solid)
    "expected": "#ff7f0e",  # orange (dashed)
    "outgoing": "#d62728",  # red (solid)
    "in_port":  "#9467bd",  # purple (solid)
}

def add_ship_segment(row, layer, mode: str):
    name = row.get("name","")
    lp   = row.get("route_last_port") or row.get("last_port_detailed") or row.get("last_port") or ""
    dest = row.get("destination") or ("Berbera, Somaliland" if mode in ("incoming","in_port","expected") else "")
    b    = get_bearing(row)
    lat  = row.get("lat_deg")
    lon  = row.get("lon_deg")

    # normalize popup text for berbera
    lp = fix_berbera(lp)
    dest = fix_berbera(dest)

    color = STATUS_COLORS.get(mode, "#333333")

    # popup text
    b_txt = (f"<br>Bearing: {round(b)}°" if b is not None else "")
    if mode == "incoming":
        pop = f"<b>{name}</b><br>From: {lp}<br>To: {dest}{b_txt}"
        arrow_text = "  ▶  ▶  ▶  "   # -->
        dashed = False
    elif mode == "outgoing":
        pop = f"<b>{name}</b><br>To: {dest}{b_txt}"
        arrow_text = "  ◀  ◀  ◀  "   # <--
        dashed = False
    elif mode == "expected":
        pop = f"<b>{name}</b><br>Status: Expected<br>From: {lp}<br>To: {dest}"
        arrow_text = "  ▶  ▶  ▶  "   # -->
        dashed = True
    else:  # in_port
        pop = f"<b>{name}</b><br>Status: In port<br>From: {lp or '—'}"
        arrow_text = ""  # no arrows for berth line
        dashed = False

    def poly_with_arrows(points, dash=False):
        pl = folium.PolyLine(points, weight=5, opacity=0.9, color=color,
                             tooltip=name, popup=pop,
                             dash_array=("8 6" if dash else None))
        pl.add_to(layer)
        if arrow_text:
            PolyLineTextPath(
                pl, arrow_text, repeat=True, offset=7,
                attributes={'font-weight': 'bold', 'font-size': '14'}
            ).add_to(layer)

    # 1) Live lat/lon: draw the true segment
    if pd.notna(lat) and pd.notna(lon):
        if mode in ("incoming","expected","in_port"):
            pts = [(lat, lon), (BERBERA_LAT, BERBERA_LON)]
        elif mode == "outgoing":
            pts = [(BERBERA_LAT, BERBERA_LON), (lat, lon)]
        poly_with_arrows(pts, dash=dashed)
        folium.CircleMarker(location=(lat, lon), radius=5, tooltip=name, popup=pop,
                            color=color, fill=True).add_to(layer)
        return

    # 2) No position: try last port / destination coordinates
    if mode in ("incoming","expected","in_port"):
        port = port_lookup(lp)
        if port:
            plat, plon, _ = port
            pts = [(plat, plon), (BERBERA_LAT, BERBERA_LON)]
            poly_with_arrows(pts, dash=dashed)
            folium.CircleMarker(location=(plat, plon), radius=5, tooltip=f"{lp}",
                                popup=f"{lp}", color=color, fill=True).add_to(layer)
            return
    elif mode == "outgoing":
        port = port_lookup(dest)
        if port:
            dlat, dlon, _ = port
            pts = [(BERBERA_LAT, BERBERA_LON), (dlat, dlon)]
            poly_with_arrows(pts, dash=dashed)
            folium.CircleMarker(location=(dlat, dlon), radius=5, tooltip=f"{dest}",
                                popup=f"{dest}", color=color, fill=True).add_to(layer)
            return

    # 3) Still nothing: draw a short bearing ray from Berbera
    if b is not None:
        ray = direction_ray(BERBERA_LAT, BERBERA_LON, b, length_km=75 if mode=="outgoing" else 65)
        poly_with_arrows(ray, dash=dashed)
    else:
        if mode == "in_port":
            folium.CircleMarker(location=(BERBERA_LAT, BERBERA_LON), radius=4,
                                tooltip=name, popup=pop, color=color, fill=True).add_to(layer)

# Draw ship layers
for _, r in incoming_df.iterrows(): add_ship_segment(r, fg_in,  "incoming")
for _, r in expected_df.iterrows(): add_ship_segment(r, fg_exp, "expected")
for _, r in outgoing_df.iterrows(): add_ship_segment(r, fg_out, "outgoing")
for _, r in inport_df.iterrows():   add_ship_segment(r, fg_berth,"in_port")

# Optional: Heat map of historical in-port origins (density of last ports)
with st.expander("Heat Layer Options"):
    show_heat = st.checkbox("Show heat map of historical in-port origins (density of last ports)")

if show_heat:
    df_in_hist = df_all[(df_all["status"]=="in_port")].copy()
    if not df_in_hist.empty:
        df_in_hist = add_derived_fields(df_in_hist)
        heat_points = []
        for _, rr in df_in_hist.iterrows():
            lp = rr.get("route_last_port") or rr.get("last_port_detailed") or rr.get("last_port")
            lp = fix_berbera(lp)
            if not lp:
                continue
            port = port_lookup(lp)
            if port:
                plat, plon, _ = port
                w = float(rr.get("teu_equiv") or 1.0)
                heat_points.append([plat, plon, max(w, 1.0)])
        if heat_points:
            HeatMap(heat_points, radius=18, blur=22, max_zoom=10, min_opacity=0.3).add_to(fg_heat)

# Add layers & controls
fg_in.add_to(m)
fg_exp.add_to(m)
fg_out.add_to(m)
fg_berth.add_to(m)
if show_heat:
    fg_heat.add_to(m)

MiniMap(toggle_display=True, minimized=True).add_to(m)
folium.LayerControl(collapsed=False).add_to(m)

st.components.v1.html(m._repr_html_(), height=560, scrolling=False)
st.caption("Legend — Incoming (Arrivals): solid green ▶, Expected: dashed orange ▶, Outgoing: solid red ◀, In-port: solid purple. Heat layer shows historical origin density. All 'Berbera, Somalia' normalized to 'Berbera, Somaliland'.")

st.markdown("---")

# =========================
# Historical In-Port Browser (snapshots) — SAFE COLS
# =========================
st.subheader("Historical In-Port Browser (tug-free snapshots)")
df_in_hist = df_all[(df_all["status"]=="in_port") & df_all["scraped_at_utc"].notna()].copy()
if df_in_hist.empty:
    st.info("No in-port history yet.")
else:
    df_in_hist = add_derived_fields(df_in_hist)
    df_in_hist["scraped_floor"] = pd.to_datetime(df_in_hist["scraped_at_utc"], utc=True, errors="coerce").dt.floor("s")
    unique_times = sorted(df_in_hist["scraped_floor"].dropna().unique())
    chosen_ts = st.selectbox("Snapshot time (UTC)", unique_times, index=len(unique_times)-1)
    snap = df_in_hist[df_in_hist["scraped_floor"] == chosen_ts].copy()
    st.caption(f"In port @ {chosen_ts} — {snap['mmsi'].nunique()} vessels | {pd.to_numeric(snap.get('teu_equiv', pd.Series(dtype=float)), errors='coerce').fillna(0).sum():,.0f} TEU-equiv")
    cols_hist = ["name","mmsi","ship_type","status","route_last_port","destination","gt","dwt","length_m","beam_m","teu_equiv","scraped_at_utc"]
    st.dataframe(snap[cols_present(snap, cols_hist)], use_container_width=True, hide_index=True)

st.markdown("---")

# =========================
# Port-Call Analytics
# =========================
st.subheader("Port Calls (arrival-based, counted once)")
if (pc := port_calls).empty:
    st.info("No detected port calls yet.")
else:
    pc["route_last_port"] = pc["last_port_detailed"].fillna(pc["last_port"])
    st.markdown("### Currently Berthed (live dwell)")
    active = pc[pc["departed_at"].isna()].copy()
    if active.empty:
        st.write("No active calls right now.")
    else:
        active["dwell_hours"] = active["dwell_hours"].round(1)
        st.dataframe(
            active[cols_present(active, ["name","mmsi","ship_type","route_last_port","arrived_at","dwell_hours","call_teu","sla_label"])],
            use_container_width=True, hide_index=True
        )

    st.markdown("### Completed Calls Leaderboard")
    completed = pc[pc["completed"]].copy()
    if completed.empty:
        st.write("No completed calls yet.")
    else:
        completed["dwell_hours"] = completed["dwell_hours"].round(1)
        n_show = st.slider("How many to show", min_value=5, max_value=50, value=10, step=5)
        fastest = completed.sort_values("dwell_hours", ascending=True).head(n_show)
        slowest = completed.sort_values("dwell_hours", ascending=False).head(n_show)

        cA, cB = st.columns(2)
        with cA:
            st.caption("Fastest unloads (shortest dwell)")
            st.dataframe(
                fastest[cols_present(fastest, ["name","mmsi","ship_type","route_last_port","arrived_at","departed_at","dwell_hours","call_teu","sla_label"])],
                use_container_width=True, hide_index=True
            )
        with cB:
            st.caption("Slowest unloads (longest dwell)")
            st.dataframe(
                slowest[cols_present(slowest, ["name","mmsi","ship_type","route_last_port","arrived_at","departed_at","dwell_hours","call_teu","sla_label"])],
                use_container_width=True, hide_index=True
            )

        st.markdown("### Dwell Distribution (Completed Calls)")
        fig = px.histogram(completed, x="dwell_hours", nbins=20, title="Dwell Hours Histogram (Completed Calls)")
        st.plotly_chart(fig, use_container_width=True)

# =========================
# Methodology
# =========================
with st.expander("ℹ️ Methodology"):
    st.markdown(f"""
**Map logic**
- **Live position** → plot ship point and segment **to/from Berbera Somaliland**.
- **No position** → try **Last Port/Destination** via internal port coordinates and draw a geodesic.
- **Still none** → draw a **bearing ray** using Course°/cardinal.

**Line styles**
- **Incoming (Arrivals)**: solid green ▶▶▶
- **Expected**: dashed orange ▶▶▶
- **Outgoing**: solid red ◀◀◀
- **In-Port**: solid purple berth/line
- **Heat layer**: density of **historical last ports** for in-port snapshots.

**Normalization**
- Any occurrence of **'Berbera, Somalia'** is shown as **'Berbera, Somaliland'**.

**Port-calls**
- Arrival = first `in_port` after away; TEU counted **once** per arrival (max during berth).
""")
