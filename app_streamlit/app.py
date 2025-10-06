import os, pandas as pd, psycopg2
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Berbera Port Monitor — SOBBO", layout="wide")
DATABASE_URL = st.secrets["DATABASE_URL"]  # set in Streamlit Cloud

@st.cache_data(ttl=60)
def q(sql, params=None):
    conn = psycopg2.connect(DATABASE_URL)
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

st.title("Berbera Port Monitor (Free AIS)")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Alongside now",
          int(q("SELECT COUNT(*) AS c FROM port_calls WHERE departure_at IS NULL").iloc[0,0]))
c2.metric("Inbound (dest→BERBERA)",
          int(q("""
             WITH latest AS (
               SELECT DISTINCT ON (mmsi) mmsi, destination, received_at
               FROM ais_positions ORDER BY mmsi, received_at DESC
             )
             SELECT COUNT(*) FROM latest
             WHERE destination ILIKE '%BERBERA%' OR destination ILIKE '%SOBBO%' OR destination ILIKE '%BBO%';
          """).iloc[0,0]))
c3.metric("Departed (24h)",
          int(q("SELECT COUNT(*) FROM port_calls WHERE departure_at > now() - interval '24 hours'").iloc[0,0]))
avg_wait = q("""
   SELECT ROUND(COALESCE(AVG(waiting_minutes),0)) AS m
   FROM port_calls WHERE arrival_at > now() - interval '30 days'
""").iloc[0,0]
c4.metric("Avg waiting (30d)", f"{int(avg_wait)} min")

st.markdown("---")

gran = st.radio("Aggregation", ["Weekly","Monthly"], index=1, horizontal=True)
ts = q("""
    SELECT date_trunc(%s, arrival_at) AS period, COUNT(*) AS calls
    FROM port_calls GROUP BY 1 ORDER BY 1
""", ( 'week' if gran=="Weekly" else 'month', ))
fig = px.line(ts, x="period", y="calls", title=f"Port calls ({gran})")
st.plotly_chart(fig, use_container_width=True)

yoy = q("""
WITH m AS (
  SELECT date_trunc('month', arrival_at)::date AS mth, COUNT(*) AS calls
  FROM port_calls GROUP BY 1
),
cur AS (SELECT calls FROM m WHERE mth = date_trunc('month', now())::date),
ly  AS (SELECT calls FROM m WHERE mth = (date_trunc('month', now()) - interval '1 year')::date)
SELECT COALESCE((SELECT calls FROM cur),0) AS this_month,
       COALESCE((SELECT calls FROM ly),0) AS last_year_same_month,
       COALESCE((SELECT calls FROM cur),0) - COALESCE((SELECT calls FROM ly),0) AS delta
""")
st.subheader("This month vs same month last year")
st.dataframe(yoy)
