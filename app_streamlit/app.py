# app_streamlit/app.py

import os
import pandas as pd
import psycopg2
import streamlit as st

# ---------------------------
# Page config
# ---------------------------
st.set_page_config(page_title="Berbera Port Monitor (Free AIS/VF)", layout="wide")

st.title("Berbera Port Monitor (Free AIS / VesselFinder snapshot)")

# ---------------------------
# Database helper
# ---------------------------
# Read from env first (GitHub/locally), then Streamlit secrets (Cloud/OpenSpace)
DATABASE_URL = (
    (os.environ.get("DATABASE_URL") or "").strip().strip('"').strip("'")
    or st.secrets.get("DATABASE_URL")
)

@st.cache_data(ttl=60)
def q(sql: str, params=None) -> pd.DataFrame:
    """
    Query helper: returns a pandas DataFrame.
    Stops app with a friendly message if DATABASE_URL is missing.
    """
    if not DATABASE_URL:
        st.error(
            "DATABASE_URL is not configured.\n\n"
            "Set it in Streamlit Cloud → Manage app → Settings → Secrets.\n"
            'Example:\nDATABASE_URL = "postgresql://port_app:YOUR_PASSWORD@YOUR_NEON_HOST/berbera?sslmode=require"'
        )
        st.stop()
    conn = psycopg2.connect(DATABASE_URL)
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
    return df

# ---------------------------
# Refresh button (clears cache)
# ---------------------------
top = st.container()
with top:
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

# ---------------------------
# Debug expander (connection + VF rows)
# ---------------------------
with st.expander("🔎 Debug: Neon connection & VesselFinder rows"):
    try:
        who = q("SELECT current_user, current_database();")
        st.write("Connected as:", who.iloc[0].to_dict())

        # Count VF rows (table created by your scraper workflow)
        vf_cnt = q("SELECT COUNT(*) AS rows FROM vesselfinder_portcalls;")
        st.write("vesselfinder_portcalls rows:", int(vf_cnt["rows"].iat[0]))

        vf_latest = q("SELECT MAX(captured_at) AS max_ts FROM vesselfinder_portcalls;")
        st.write("Latest captured_at:", vf_latest["max_ts"].iat[0])
    except Exception as e:
        st.error(f"DB error: {e}")

st.markdown("---")

# ---------------------------
# VesselFinder: Latest snapshot table + KPIs
# ---------------------------
st.subheader("VesselFinder — Latest Snapshot (Berbera)")

try:
    latest_rows = q("""
    WITH max_cap AS (SELECT MAX(captured_at) AS ts FROM vesselfinder_portcalls)
    SELECT vessel_name, status, destination, eta_utc, captured_at
    FROM vesselfinder_portcalls, max_cap
    WHERE captured_at = max_cap.ts
    ORDER BY status, COALESCE(eta_utc, captured_at), vessel_name;
    """)
    if latest_rows.empty:
        st.info("No VesselFinder snapshot rows yet. Run the GitHub Action “Scrape VesselFinder (daily)” and refresh.")
    else:
        st.dataframe(latest_rows, use_container_width=True)

        kpis = q("""
        WITH max_cap AS (SELECT MAX(captured_at) AS ts FROM vesselfinder_portcalls),
        latest AS (
          SELECT * FROM vesselfinder_portcalls, max_cap
          WHERE captured_at = max_cap.ts
        )
        SELECT
          SUM((status = 'expected')::int)   AS expected_now,
          SUM((status = 'in_port')::int)    AS in_port_now,
          SUM((status = 'arrivals')::int)   AS arrivals_listed,
          SUM((status = 'departures')::int) AS departures_listed
        FROM latest;
        """)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Expected (VF)",          int(kpis["expected_now"].iat[0] or 0))
        c2.metric("In port (VF)",           int(kpis["in_port_now"].iat[0] or 0))
        c3.metric("Arrivals listed (VF)",   int(kpis["arrivals_listed"].iat[0] or 0))
        c4.metric("Departures listed (VF)", int(kpis["departures_listed"].iat[0] or 0))
except Exception as e:
    st.error(f"Error loading latest snapshot: {e}")

st.markdown("---")

# ---------------------------
# Over-time charts (distinct vessels per period)
# ---------------------------
st.subheader("Counts over time (distinct vessels per period)")

try:
    daily = q("""
    SELECT date_trunc('day', captured_at) AS day,
           COUNT(DISTINCT vessel_name)    AS ships
    FROM vesselfinder_portcalls
    GROUP BY 1
    ORDER BY 1
    """)
    if daily.empty:
        st.info("No daily data yet.")
    else:
        st.line_chart(daily.set_index("day")["ships"])
except Exception as e:
    st.error(f"Daily chart error: {e}")

try:
    weekly = q("""
    SELECT date_trunc('week', captured_at) AS wk,
           COUNT(DISTINCT vessel_name)     AS ships
    FROM vesselfinder_portcalls
    GROUP BY 1
    ORDER BY 1
    """)
    if weekly.empty:
        st.info("No weekly data yet.")
    else:
        st.bar_chart(weekly.set_index("wk")["ships"])
except Exception as e:
    st.error(f"Weekly chart error: {e}")

try:
    monthly = q("""
    SELECT date_trunc('month', captured_at) AS mon,
           COUNT(DISTINCT vessel_name)      AS ships
    FROM vesselfinder_portcalls
    GROUP BY 1
    ORDER BY 1
    """)
    if monthly.empty:
        st.info("No monthly data yet.")
    else:
        st.bar_chart(monthly.set_index("mon")["ships"])
except Exception as e:
    st.error(f"Monthly chart error: {e}")

st.markdown("---")

# ---------------------------
# YoY comparison (this month vs same month last year)
# ---------------------------
st.subheader("YoY compare — this month vs same month last year (VF)")

try:
    yoy = q("""
    WITH this_month AS (
      SELECT COUNT(DISTINCT vessel_name) AS c
      FROM vesselfinder_portcalls
      WHERE date_trunc('month', captured_at) = date_trunc('month', now())
    ),
    last_year AS (
      SELECT COUNT(DISTINCT vessel_name) AS c
      FROM vesselfinder_portcalls
      WHERE date_trunc('month', captured_at) = date_trunc('month', now() - interval '1 year')
    )
    SELECT (SELECT c FROM this_month) AS this_month,
           (SELECT c FROM last_year)  AS last_year_same_month;
    """)
    if yoy.empty:
        st.info("YoY data not available yet.")
    else:
        v = int(yoy["this_month"].iat[0] or 0)
        l = int(yoy["last_year_same_month"].iat[0] or 0)
        st.metric("Distinct vessels this month (VF)", value=v, delta=v - l)
except Exception as e:
    st.error(f"YoY block error: {e}")

st.markdown("---")

# ---------------------------
# Useful lists from the latest snapshot
# ---------------------------
st.subheader("Inbound (Expected → Berbera)")
try:
    inbound = q("""
    WITH max_cap AS (SELECT MAX(captured_at) AS ts FROM vesselfinder_portcalls)
    SELECT vessel_name, destination, eta_utc
    FROM vesselfinder_portcalls, max_cap
    WHERE captured_at = max_cap.ts
      AND status = 'expected'
    ORDER BY eta_utc NULLS LAST, vessel_name
    """)
    st.dataframe(inbound, use_container_width=True)
except Exception as e:
    st.error(f"Inbound list error: {e}")

st.subheader("Currently In Port (VF)")
try:
    inport = q("""
    WITH max_cap AS (SELECT MAX(captured_at) AS ts FROM vesselfinder_portcalls)
    SELECT vessel_name, destination, captured_at
    FROM vesselfinder_portcalls, max_cap
    WHERE captured_at = max_cap.ts
      AND status = 'in_port'
    ORDER BY vessel_name
    """)
    st.dataframe(inport, use_container_width=True)
except Exception as e:
    st.error(f"In-port list error: {e}")

st.markdown("---")

# ---------------------------
# (Optional) Original AIS/port_calls KPIs — only if table exists & not empty
# ---------------------------
with st.expander("🛰️ AIS-derived metrics (experimental, if port_calls populated)"):
    try:
        # Detect table existence
        exists = q("""
        SELECT COUNT(*) AS c
        FROM information_schema.tables
        WHERE table_schema='public' AND table_name='port_calls';
        """)
        if int(exists["c"].iat[0]) == 0:
            st.info("Table 'port_calls' not found yet (this section is optional).")
        else:
            # Example KPI: alongside now
            k_alongside = q("SELECT COUNT(*) AS c FROM port_calls WHERE departure_at IS NULL;")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Alongside now (AIS)", int(k_alongside["c"].iat[0] or 0))

            # Add more AIS KPIs if you later populate port_calls with your detector.
    except Exception as e:
        st.warning(f"AIS metrics skipped: {e}")
