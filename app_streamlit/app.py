# app_streamlit/app.py
# ------------------------------------------------------------
# Berbera Port Monitor — TEU (Port-Calls · Enrichment · Map)
# ------------------------------------------------------------
# - Robust against missing optional columns in history snapshots
# - Shows real polylines when lat/lon exist, else schematic rays
# - Marks in_port ships even without bearings
# ------------------------------------------------------------

import os
import io
import math
from datetime import datetime, timezone
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st
import plotly.express as px
import boto3
import folium
from folium import FeatureGroup
from folium.plugins import MiniMap

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

# --- Berbera Port approx (adjust if you have a precise pier centroid)
BERBERA_LAT = float(st.secrets.get("BERBERA_LAT", os.getenv("BERBERA_LAT", "10.4396")))
BERBERA_LON = float(st.secrets.get("BERBERA_LON", os.getenv("BERBERA_LON", "45.0143")))

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
    if "scraped_at_utc" in df.columns:
        df["scraped_at_utc"] = pd.to_datetime(df["scraped_at_utc"], errors="coerce", utc=True)
    if "eta_to_berbera_utc" in df.columns:
        df["eta_to_berbera_utc"] = pd.to_datetime(df["eta_to_berbera_utc"], errors="coerce", utc=True)
    if "atd_last_port_utc" in df.columns:
        df["atd_last_port_utc"] = pd.to_datetime(df["atd_last_port_utc"], errors="coerce", utc=True)
    return df

def unify_schema(df: pd.DataFrame) -> pd.DataFrame:
    needed = [
        "scraped_at_utc","name","mmsi","ship_type","status","last_port",
        "distance_nm_to_berbera","eta_to_berbera_utc","speed_kn",
        "gt","dwt","length_m","beam_m","built_year",
        "teu_capacity_actual","teu_equiv","source",
        # enrichment fields (may or may not be present in older history)
        "detail_url","destination","last_port_detailed","atd_last_port_utc",
        "course_deg","heading_deg","nav_status","direction_cardinal",
        "position_age_min","flag","imo","callsign","lat_deg","lon_deg",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = None
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["ship_type"] = df["ship_type"].astype(str).str.strip().str.title()
    df = coerce_timestamps(df)
    for c in ["distance_nm_to_berbera","speed_kn","gt","dwt","length_m","beam_m","built_year",
              "teu_equiv","teu_capacity_actual","course_deg","heading_deg",
              "position_age_min","lat_deg","lon_deg"]:
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

# TEU fallbacks (aligned with scraper)
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

# =========================
# Helpers
# =========================
def safe_cols(df: pd.DataFrame, wanted: list[str]) -> list[str]:
    return [c for c in wanted if c in df.columns]

def compute_latest_rows() -> pd.DataFrame:
    if not vf_latest.empty:
        df = unify_schema(vf_latest.copy()); df = _exclude_tugs(df); df = ensure_teu_equiv(df); return df
    if not df_all.empty:
        max_ts = df_all["scraped_at_utc"].max(); return df_all[df_all["scraped_at_utc"] == max_ts]
    return pd.DataFrame()

# =========================
# Port-call detection + SLA
# =========================
@st.cache_data(ttl=0)
def build_port_calls(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "mmsi","name","ship_type","arrived_at","departed_at","dwell_hours",
            "call_teu","last_port","last_port_detailed"
        ])
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
                in_call, call_start = True, ts; call_teus=[teu]
                if pd.notna(lp):  lps.append(lp)
                if pd.notna(lpd): lpds.append(lpd)
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
st.markdown("---")

# =========================
# Current status (enriched)
# =========================
statuses_present = sorted(set(x for x in df_all["status"].dropna().unique() if x in KNOWN_STATUSES)) or KNOWN_STATUSES
status = st.selectbox("View", statuses_present, index=0)
types_all = sorted(df_all["ship_type"].dropna().unique())
selected_types = st.multiselect("Ship types", types_all, default=types_all)

latest_df = latest_rows.copy()
if not latest_df.empty:
    if status: latest_df = latest_df[latest_df["status"] == status]
    if selected_types: latest_df = latest_df[latest_df["ship_type"].isin(selected_types)]

latest_df["route_last_port"] = latest_df["last_port_detailed"].fillna(latest_df["last_port"])
latest_df["dir"] = latest_df["direction_cardinal"].fillna("")

cols_latest = [
    "name","mmsi","ship_type","status","route_last_port","destination","dir",
    "gt","dwt","length_m","beam_m","teu_equiv","eta_to_berbera_utc",
    "speed_kn","course_deg","lat_deg","lon_deg","scraped_at_utc","detail_url"
]
st.subheader(f"Latest — {status} (tug-free, enriched)")
st.dataframe(latest_df[safe_cols(latest_df, cols_latest)], use_container_width=True, hide_index=True)
st.download_button("⬇️ Download CSV", latest_df[safe_cols(latest_df, cols_latest)].to_csv(index=False).encode("utf-8"),
                   file_name=f"{status}_latest_teu.csv", mime="text/csv")

st.markdown("---")

# =========================
# Directional Map — Berbera vicinity
# =========================
st.subheader("Map — Berbera & Vicinity (Real Segments when possible)")

def bearing_from_cardinal(card: str) -> Optional[float]:
    m = {"N":0,"NE":45,"E":90,"SE":135,"S":180,"SW":225,"W":270,"NW":315}
    return m.get(str(card).upper()) if isinstance(card, str) else None

def destination_point(lat, lon, bearing_deg, distance_km):
    R = 6371.0088  # km
    φ1 = math.radians(lat); λ1 = math.radians(lon); θ = math.radians(bearing_deg); d = distance_km
    φ2 = math.asin(math.sin(φ1)*math.cos(d/R) + math.cos(φ1)*math.sin(d/R)*math.cos(θ))
    λ2 = λ1 + math.atan2(math.sin(θ)*math.sin(d/R)*math.cos(φ1),
                         math.cos(d/R)-math.sin(φ1)*math.sin(φ2))
    return (math.degrees(φ2), ((math.degrees(λ2)+540)%360)-180)

def direction_ray(lat0, lon0, bearing_deg, length_km=60):
    lat1, lon1 = destination_point(lat0, lon0, bearing_deg, length_km)
    return [(lat0, lon0), (lat1, lon1)]

incoming_df = latest_rows[(latest_rows["status"]=="incoming")].copy()
outgoing_df = latest_rows[(latest_rows["status"]=="outgoing")].copy()
inport_df   = latest_rows[(latest_rows["status"]=="in_port")].copy()

def get_bearing(row):
    if pd.notna(row.get("course_deg")):
        return float(row["course_deg"])
    card = row.get("direction_cardinal")
    b = bearing_from_cardinal(card) if pd.notna(card) else None
    return b

m = folium.Map(location=[BERBERA_LAT, BERBERA_LON], tiles="OpenStreetMap", zoom_start=10, control_scale=True)

folium.Marker([BERBERA_LAT, BERBERA_LON],
              tooltip="Berbera Port",
              popup="Berbera Port",
              icon=folium.Icon(color="blue", icon="anchor", prefix="fa")).add_to(m)

fg_in  = FeatureGroup(name="Incoming", show=True)
fg_out = FeatureGroup(name="Outgoing", show=True)
fg_berth = FeatureGroup(name="In-Port markers", show=True)

def add_ship_segment_or_ray(row, layer, mode: str):
    """
    mode = 'incoming' or 'outgoing'
    If row has lat_deg/lon_deg, draw a real segment; else fallback to schematic ray.
    """
    name = row.get("name","")
    lp   = row.get("last_port_detailed") or row.get("last_port") or ""
    dest = row.get("destination") or ("Berbera" if mode=="incoming" else "")
    b    = get_bearing(row)
    lat  = row.get("lat_deg")
    lon  = row.get("lon_deg")

    if mode == "incoming":
        pop = f"<b>{name}</b><br>From: {lp}<br>To: {dest}{('<br>Bearing: %d°'%round(b)) if b is not None else ''}"
        color = "#2ca02c"
    else:
        pop = f"<b>{name}</b><br>To: {dest}{('<br>Bearing: %d°'%round(b)) if b is not None else ''}"
        color = "#d62728"

    if pd.notna(lat) and pd.notna(lon):
        pts = ([(lat, lon), (BERBERA_LAT, BERBERA_LON)] if mode=="incoming"
               else [(BERBERA_LAT, BERBERA_LON), (lat, lon)])
        folium.PolyLine(pts, weight=5, opacity=0.8, color=color, tooltip=name, popup=pop).add_to(layer)
        folium.CircleMarker(location=(lat, lon), radius=5, tooltip=name, popup=pop, color=color, fill=True).add_to(layer)
    else:
        if b is None:
            return
        ray = direction_ray(BERBERA_LAT, BERBERA_LON, b, length_km=75 if mode=="outgoing" else 65)
        folium.PolyLine(ray, weight=5, opacity=0.7, color=color, tooltip=name, popup=pop,
                        dash_array="8 6" if mode=="outgoing" else None).add_to(layer)

# Incoming / Outgoing layers
for _, r in incoming_df.iterrows():
    add_ship_segment_or_ray(r, fg_in, mode="incoming")

for _, r in outgoing_df.iterrows():
    add_ship_segment_or_ray(r, fg_out, mode="outgoing")

# In-port markers (even without bearings)
for _, r in inport_df.iterrows():
    name = r.get("name","")
    lat  = r.get("lat_deg")
    lon  = r.get("lon_deg")
    if pd.notna(lat) and pd.notna(lon):
        pop = f"<b>{name}</b><br>Status: In Port"
        folium.CircleMarker(location=(lat, lon), radius=6, tooltip=name, popup=pop,
                            color="#1f77b4", fill=True).add_to(fg_berth)

fg_in.add_to(m); fg_out.add_to(m); fg_berth.add_to(m)
MiniMap(toggle_display=True, minimized=True).add_to(m)
folium.LayerControl(collapsed=True).add_to(m)

st.components.v1.html(m._repr_html_(), height=540, scrolling=False)

# Basic debug counts (helps when map looks empty)
st.caption(f"Map data — incoming: {len(incoming_df)}, outgoing: {len(outgoing_df)}, in_port: {len(inport_df)}")

st.markdown("---")

# =========================
# Historical In-Port Browser (tug-free snapshots)
# =========================
st.subheader("Historical In-Port Browser (tug-free snapshots)")
df_in_hist = df_all[(df_all["status"]=="in_port") & df_all["scraped_at_utc"].notna()].copy()
if df_in_hist.empty:
    st.info("No in-port history yet.")
else:
    df_in_hist["scraped_floor"] = pd.to_datetime(df_in_hist["scraped_at_utc"], utc=True, errors="coerce").dt.floor("s")
    unique_times = sorted(df_in_hist["scraped_floor"].unique())
    chosen_ts = st.selectbox("Snapshot time (UTC)", unique_times, index=len(unique_times)-1)
    snap = df_in_hist[df_in_hist["scraped_floor"] == chosen_ts].copy()

    # derive optional route_last_port if not present
    if "route_last_port" not in snap.columns:
        snap["route_last_port"] = snap["last_port_detailed"].fillna(snap["last_port"])

    want_cols = [
        "name","mmsi","ship_type","status","route_last_port","destination",
        "gt","dwt","length_m","beam_m","teu_equiv","scraped_at_utc"
    ]
    st.caption(f"In port @ {chosen_ts} — {snap['mmsi'].nunique()} vessels | {pd.to_numeric(snap['teu_equiv'], errors='coerce').fillna(0).sum():,.0f} TEU-equiv")
    st.dataframe(snap[safe_cols(snap, want_cols)], use_container_width=True, hide_index=True)

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
            active[["name","mmsi","ship_type","route_last_port","arrived_at","dwell_hours","call_teu","sla_label"]],
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
                fastest[["name","mmsi","ship_type","route_last_port","arrived_at","departed_at","dwell_hours","call_teu","sla_label"]],
                use_container_width=True, hide_index=True
            )
        with cB:
            st.caption("Slowest unloads (longest dwell)")
            st.dataframe(
                slowest[["name","mmsi","ship_type","route_last_port","arrived_at","departed_at","dwell_hours","call_teu","sla_label"]],
                use_container_width=True, hide_index=True
            )

        st.markdown("### Dwell Distribution (Completed Calls)")
        fig = px.histogram(completed, x="dwell_hours", nbins=20, title="Dwell Hours Histogram (Completed Calls)")
        st.plotly_chart(fig, use_container_width=True)

# =========================
# Direction "Heat" — Wind-Rose over time
# =========================
st.markdown("---")
st.subheader("Direction Heat — Where do ships come from? (Wind-Rose)")

hist_dir = df_all.copy()
hist_dir["bearing"] = hist_dir["course_deg"]
card_map = {"N":0,"NE":45,"E":90,"SE":135,"S":180,"SW":225,"W":270,"NW":315}
mask_no_bearing = hist_dir["bearing"].isna() & hist_dir["direction_cardinal"].notna()
hist_dir.loc[mask_no_bearing, "bearing"] = hist_dir.loc[mask_no_bearing, "direction_cardinal"].map(lambda c: card_map.get(str(c).upper()))

hist_dir = hist_dir[(hist_dir["status"].isin(["incoming","in_port"])) & hist_dir["bearing"].notna()].copy()
if hist_dir.empty:
    st.info("No direction data yet.")
else:
    def sector(b):
        b = float(b) % 360
        bins = [(0,"N"),(45,"NE"),(90,"E"),(135,"SE"),(180,"S"),(225,"SW"),(270,"W"),(315,"NW"),(360,"N")]
        for i in range(len(bins)-1):
            if b >= bins[i][0] and b < bins[i+1][0]:
                return bins[i][1]
        return "N"
    hist_dir["sector"] = hist_dir["bearing"].map(sector)

    time_choice = st.radio("Window", ["Last 7 days","Last 30 days","All"], horizontal=True)
    now = pd.Timestamp.now(tz="UTC")
    if time_choice == "Last 7 days":
        hist_dir = hist_dir[hist_dir["scraped_at_utc"] >= (now - pd.Timedelta(days=7))]
    elif time_choice == "Last 30 days":
        hist_dir = hist_dir[hist_dir["scraped_at_utc"] >= (now - pd.Timedelta(days=30))]

    rose = (hist_dir.groupby("sector", as_index=False).size().rename(columns={"size":"count"}))

    if rose.empty:
        st.info("No data in selected window.")
    else:
        fig_rose = px.bar_polar(rose, r="count", theta="sector", direction="clockwise", start_angle=90,
                                title="Direction Wind-Rose (arrivals + in-port)")
        st.plotly_chart(fig_rose, use_container_width=True)

# =========================
# Methodology
# =========================
with st.expander("ℹ️ Methodology"):
    st.markdown(f"""
**Directional Map**
- Real segments drawn when vessel coordinates are available from detail pages.
- Fallback: schematic rays using Course° (or cardinal) from detail pages.

**Port-calls**
- Count each call once on arrival; dwell until first non-in_port status.

**Targets & SLA**
- Annual target = {ANNUAL_TEU_TARGET:,.0f} TEU; SLA = {SLA_HOURS:.0f} h dwell.
""")
