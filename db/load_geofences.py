import json, os, psycopg2
from shapely.geometry import shape

DATABASE_URL = os.environ.get("DATABASE_URL")

def load_one(fid, fpath):
    with open(fpath, "r", encoding="utf-8") as f:
        gj = json.load(f)
    if gj.get("type") == "FeatureCollection":
        geom_obj = gj["features"][0]["geometry"]
    else:
        geom_obj = gj["geometry"]
    from shapely.geometry import shape as shp_shape
    geom = shp_shape(geom_obj)
    return fid, geom.wkt

def main():
    assert DATABASE_URL, "Missing DATABASE_URL env var"
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    for fid, fpath in [
        ("berbera_port", "geodata/berbera_port.geojson"),
        ("berbera_anchorage", "geodata/berbera_anchorage.geojson"),
    ]:
        fid, wkt_geom = load_one(fid, fpath)
        cur.execute(
            """
            INSERT INTO geofences (id, geom)
            VALUES (%s, ST_GeomFromText(%s, 4326))
            ON CONFLICT (id) DO UPDATE SET geom = EXCLUDED.geom;
            """,
            (fid, wkt_geom),
        )
    print("✅ Geofences loaded.")

if __name__ == "__main__":
    main()
