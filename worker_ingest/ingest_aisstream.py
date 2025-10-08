# worker_ingest/ingest_aisstream.py

import os, json, asyncio, websockets, psycopg2, time
from datetime import datetime, timezone

# --- Clean and validate secrets ---
AISS_API_KEY = (os.environ.get("AISS_API_KEY") or "").strip().strip('"').strip("'")
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip().strip('"').strip("'")

if not AISS_API_KEY:
    raise SystemExit("❌ AISS_API_KEY is missing. Set it in GitHub → Settings → Secrets → Actions.")
if not DATABASE_URL:
    raise SystemExit("❌ DATABASE_URL is missing. Set it in GitHub → Settings → Secrets → Actions.")

# Bounding box around Berbera: [minLon, minLat, maxLon, maxLat]
# If you see no data after a few runs, widen slightly (e.g., [44.5, 10.0, 45.5, 11.0]).
BBOX = [44.95, 10.35, 45.10, 10.50]

# Run each Action invocation for ~2 minutes, then exit (prevents overlap)
RUN_SECONDS = 120

async def main():
    uri = "wss://stream.aisstream.io/v0/stream"
    subscription = {
        "APIKey": AISS_API_KEY,
        "BoundingBoxes": [[BBOX]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
    }

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    start = time.time()
    # Robust websocket with periodic resubscribe to keep alive
    async with websockets.connect(uri, ping_interval=20) as ws:
        await ws.send(json.dumps(subscription))
        while True:
            # stop after RUN_SECONDS so the workflow ends cleanly
            if time.time() - start > RUN_SECONDS:
                print("⏱️ Ingest window complete; exiting.")
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
            except asyncio.TimeoutError:
                # Re-send subscription to keep stream alive during quiet periods
                await ws.send(json.dumps(subscription))
                continue

            msg = json.loads(raw)

            # We only store dynamic position reports for now
            if msg.get("MessageType") == "PositionReport":
                d = msg["Message"]
                mmsi = d.get("UserID")
                lat  = d.get("Latitude")
                lon  = d.get("Longitude")
                sog  = d.get("SOG")
                cog  = d.get("COG")
                nav  = d.get("NavigationalStatus")
                ts   = datetime.now(timezone.utc)

                # Insert into ais_positions with a PostGIS point
                cur.execute("""
                    INSERT INTO ais_positions
                      (mmsi, received_at, lat, lon, sog, cog, nav_status, geom)
                    VALUES
                      (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                """, (mmsi, ts, lat, lon, sog, cog, nav, lon, lat))

if __name__ == "__main__":
    asyncio.run(main())
