# Berbera Port Monitor (AIS api → Postgres → Streamlit)

Live KPIs for Berbera (SOBBO): inbound, alongside, departures, queue/wait, weekly/monthly, and YoY.

## Stack (all free tiers)
- **aisstream.io** WebSocket → live AIS
- **Neon** Postgres with **PostGIS** (free tier)
- **GitHub Actions** → ingest + detect schedulers
- **Streamlit Community Cloud** → web app

## Quick start
1. Create Neon DB `berbera`, enable PostGIS, create role `port_app`.
2. Run `db/schema.sql`.
3. Place accurate polygons at `geodata/berbera_port.geojson` and `geodata/berbera_anchorage.geojson`.
4. `DATABASE_URL=... python db/load_geofences.py` to load them.
5. Add repo secrets: `DATABASE_URL`, `AISS_API_KEY`.
6. Enable GitHub Actions. Data will start flowing.
7. Deploy `app_streamlit/app.py` on Streamlit and add the same `DATABASE_URL` secret.

## Notes
- Destination parsing uses `ILIKE '%BERBERA%' OR '%SOBBO%' OR '%BBO%'`.
- Clean old `ais_positions` periodically; keep `port_calls` long-term.
- Replace placeholder polygons with precise ones from QGIS.
