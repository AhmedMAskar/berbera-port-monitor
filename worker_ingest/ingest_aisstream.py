# worker_ingest/ingest_aisstream.py

import os, json, asyncio, websockets, psycopg2, time
from datetime import datetime, timezone

# --- Clean and validate secrets (protects against stray quotes/newlines) ---
AISS_API_KEY = (os.environ.get("AISS_API_KEY") or "").strip().strip('"').strip("'")
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip().strip('"').strip("'")

if not AISS_API_KEY:
    raise SystemExit("❌ AISS_API_KEY is missing. Set it in GitHub → Settings → Secrets → Actions.")
if not DATABASE_URL:
    raise SystemExit("❌ DATABASE_URL is missing. Set it in GitHub → Settings → Secrets → Actions.")

# Bounding box around Berbera: [minLon, minLat, maxLon, maxLat]
# If you see little/no data during the window, temporarily widen it, e.g.:
# BBOX = [44.5, 10.0, 45.5, 11.0]
BBOX = [44.95, 10.35, 45.10, 10.50]

# Run each Action for ~10 minutes so we actually collect a chunk
RUN_SECONDS = 600

async def main():
    uri = "wss://stream.aisstream.io/v0/stream"
    subscription = {
        "APIKey": AISS_API_KEY,
        "BoundingBoxes": [[BBOX]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    # --- DB connect + sanity prints (helps diagnose privilege issues) ---
    print("Connecting to DB…")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT current_user, current_database();")
    who, db = cur.fetchone()
    print(f"✅ DB connected as user={who}, db={db}")

    # Ensure 'ships' table exists for static data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ships (
          mmsi        bigint PRIMARY KEY,
          shipname    text,
          callsign    text,
          imo         bigint,
          shiptype    text,
          destination text,
          updated_at  timestamptz NOT NULL DEFAULT now()
        );
    """)
    print("✅ Verified 'ships' table exists")

    start = time.time()

    # Robust websocket with periodic resubscribe to keep alive during quiet periods
    async with websockets.connect(uri, ping_interval=20) as ws:
        await ws.send(json.dumps(subscription))
        print(f"Subscribed to AIS stream with BBOX={BBOX} for ~{RUN_SECONDS}s")

        while True:
            if time.time() - start > RUN_SECONDS:
                print("⏱️ Ingest window complete; exiting.")
                break

            try:
                # Wait for a message, but nudge the stream if it's quiet
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
            except asyncio.TimeoutError:
                # Re-send subscription to keep the stream alive (quiet periods happen)
                await ws.send(json.dumps(subscription))
                continue

            try:
                msg = json.loads(raw)
            except Exception as e:
                print("⚠️ JSON parse error:", repr(e))
                continue

            mtype = msg.get("MessageType")

            # -------- ShipStaticData: upsert into 'ships' --------
            if mtype == "ShipStaticData":
                s = msg.get("Message", {})
                mmsi  = s.get("UserID")
                name  = s.get("Name")
                calls = s.get("CallSign")
                imo   = s.get("IMO")
                stype = s.get("ShipType")      # e.g., "Cargo"
                dest  = s.get("Destination")

                if mmsi:
                    try:
                        cur.execute("""
                            INSERT INTO ships (mmsi, shipname, callsign, imo, shiptype, destination, updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s, now())
                            ON CONFLICT (mmsi) DO UPDATE
                            SET shipname=EXCLUDED.shipname,
                                callsign=EXCLUDED.callsign,
                                imo=EXCLUDED.imo,
                                shiptype=EXCLUDED.shiptype,
                                destination=COALESCE(EXCLUDED.destination, ships.destination),
                                updated_at=now();
                        """, (mmsi, name, calls, imo, stype, dest))
                    except Exception as e:
                        print("❌ UPSERT ships failed:", repr(e))
                        raise
                continue  # done with this message

            # -------- PositionReport: insert into 'ais_positions' --------
            if mtype == "PositionReport":
                d = msg.get("Message", {})
                mmsi = d.get("UserID")
                lat  = d.get("Latitude")
                lon  = d.get("Longitude")
                sog  = d.get("SOG")
                cog  = d.get("COG")
                nav  = d.get("NavigationalStatus")
                ts   = datetime.now(timezone.utc)

                # Skip if essential fields missing
                if mmsi is None or lat is None or lon is None:
                    continue

                try:
                    cur.execute(
                        """
                        INSERT INTO ais_positions
                          (mmsi, received_at, lat, lon, sog, cog, nav_status, geom)
                        VALUES
                          (%s, %s, %s, %s, %s, %s, %s,
                           ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                        """,
                        (mmsi, ts, lat, lon, sog, cog, nav, lon, lat),
                    )
                except Exception as e:
                    # Print the exact DB error so Actions logs show what's wrong
                    print("❌ INSERT ais_positions failed:", repr(e))
                    raise

    # Clean close (optional; autocommit enabled)
    try:
        cur.close()
        conn.close()
    except Exception:
        pass

if __name__ == "__main__":
    asyncio.run(main())
