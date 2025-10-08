# worker_detect/detect_events.py

import os, psycopg2

# --- Clean and validate secret from GitHub Actions env ---
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip().strip('"').strip("'")
if not DATABASE_URL:
    raise SystemExit("❌ DATABASE_URL is missing. Set it in GitHub → Settings → Secrets → Actions.")

SQL_RECENT = """
WITH port AS (SELECT geom FROM geofences WHERE id='berbera_port'),
     anch AS (SELECT geom FROM geofences WHERE id='berbera_anchorage'),
     latest AS (
       SELECT DISTINCT ON (mmsi) mmsi, received_at, sog, nav_status, geom
       FROM ais_positions
       ORDER BY mmsi, received_at DESC
     )
SELECT
  l.mmsi,
  l.received_at,
  COALESCE(l.sog, 99) AS sog,
  COALESCE(l.nav_status, '') AS nav_status,
  ST_Contains((SELECT geom FROM port), l.geom) AS in_port,
  ST_Contains((SELECT geom FROM anch), l.geom) AS in_anch
FROM latest l;
"""

def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(SQL_RECENT)
    for mmsi, ts, sog, nav, in_port, in_anch in cur.fetchall():
        # Is there an open (not departed) call?
        cur.execute(
            "SELECT id FROM port_calls WHERE mmsi=%s AND departure_at IS NULL ORDER BY arrival_at DESC LIMIT 1",
            (mmsi,)
        )
        open_call = cur.fetchone()

        if in_port:
            slow_or_moored = (sog < 1.0) or ("moor" in nav.lower())
            if slow_or_moored and not open_call:
                # Approx waiting: minutes since last seen in anchorage
                cur.execute(
                    """
                    WITH last_anch AS (
                      SELECT received_at FROM ais_positions
                      WHERE mmsi=%s
                        AND ST_Contains((SELECT geom FROM geofences WHERE id='berbera_anchorage'), geom)
                      ORDER BY received_at DESC LIMIT 1
                    )
                    INSERT INTO port_calls (mmsi, arrival_at, waiting_minutes)
                    VALUES (%s, now(),
                      COALESCE((SELECT EXTRACT(EPOCH FROM (now() - received_at))/60 FROM last_anch),0)::int
                    )
                    RETURNING id
                    """,
                    (mmsi, mmsi)
                )
        else:
            # If moved out of port and is making way, close the call
            if open_call and sog > 1.0:
                cur.execute("UPDATE port_calls SET departure_at = now() WHERE id=%s", (open_call[0],))

    print("✅ Detect cycle complete.")

if __name__ == "__main__":
    main()
