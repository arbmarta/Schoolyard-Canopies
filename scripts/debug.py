"""
Print CRS of GeoJSON files and output GPKG.
"""

import fiona
from pathlib import Path

# ---------- CONFIG ----------
BUILDING_DIR = Path("../building_data/LoD1/northamerica")
OUTPUT_GPKG = Path("../outputs/buildings_near_schools.gpkg")
# ----------------------------

print("GeoJSON CRS:")
for geojson in sorted(BUILDING_DIR.glob("w085_n55_w080_n50.geojson")):
    with fiona.open(str(geojson), "r") as src:
        print(f"  {geojson.name}: {src.crs}")

print(f"\nOutput GPKG CRS:")
if OUTPUT_GPKG.exists():
    with fiona.open(str(OUTPUT_GPKG), layer="buildings") as src:
        print(f"  {OUTPUT_GPKG.name}: {src.crs}")
else:
    print(f"  {OUTPUT_GPKG.name}: File not found")


"""
Output GPKG CRS:
  buildings_near_schools.gpkg: EPSG:4326
GeoJSON CRS:
"""