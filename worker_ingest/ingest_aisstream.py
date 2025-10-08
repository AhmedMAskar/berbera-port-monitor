import os, json, asyncio, websockets, psycopg2, time
from datetime import datetime, timezone

# --- Clean and validate secrets ---
AISS_API_KEY = (os.environ.get("AISS_API_KEY") or "").strip().strip('"').strip("'")
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip().strip('"').strip("'")

if not AISS_API_KEY:
    raise SystemExit("❌ AISS_API_KEY is missing. Set it in GitHub → Settings → Secrets → Actions.")
if not DATABASE_URL:
    raise SystemExit("❌ DATABASE_URL is missing. Set it in GitHub → Settings → Secrets → Actions.")


# Tight bbox around Berbera (tune as needed): [minLon, minLat, maxLon, maxLat]
BBOX = [44.95, 10.35, 45.10, 10.50]

async def main():
    assert AISS_API_KEY and DATABASE_URL, "Missing AISS_API_KEY or DATABASE_URL"
    uri = "wss://stream.aisstream.io/v0/stream"
    sub = {
        "APIKey": AISS_API_KEY,
        "BoundingBoxes": [[BBOX]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
    }
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    async with websockets.connect(uri, ping_interval=20) as ws:
        await ws.send(json.dumps(sub))
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg.get("MessageType") == "PositionReport":
                d = msg["Message"]
                mmsi = d.get("UserID")
                lat  = d.get("Latitude")
                lon  = d.get("Longitude")
                sog  = d.get("SOG")
                cog  = d.get("COG")
                nav  = d.get("NavigationalStatus")
                ts   = datetime.now(timezone.utc)
                cur.execute(
                    """
                    INSERT INTO ais_positions (mmsi, received_at, lat, lon, sog, cog, nav_status, geom)
                    VALUES (%s,%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))
                    """,
                    (mmsi, ts, lat, lon, sog, cog, nav, lon, lat)
                )

if __name__ == "__main__":
    asyncio.run(main())
