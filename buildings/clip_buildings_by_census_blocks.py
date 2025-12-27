"""
Stream-process building tiles as they download.
Filter buildings to only those within school census blocks.
Append filtered results to a single growing GPKG.

Handles:
- Tiles spanning US/Canada border
- Files still downloading in background
- Large tiles (7-8 GB)
"""

import duckdb
from pathlib import Path
import time
import traceback
import fiona
from pyproj import CRS
import geopandas as gpd
import os
import pandas as pd

# ---------- CONFIG ----------
BUILDING_DIR = Path("building_data/LoD1/northamerica")
US_CENSUS = Path("../census_blocks/united_states/US_census_blocks_with_schools.gpkg")
CANADA_CENSUS = Path("../census_blocks/canada/Canada_census_blocks_with_schools.gpkg")

OUTPUT_GPKG = Path("../buildings_near_schools.gpkg")
OUTPUT_CRS = "EPSG:4326"
N_THREADS = 4
FILE_STABILITY_WAIT = 15 # 15 second wait time to check file stability
MIN_FILE_SIZE = 1000 # Processes files at least 1 MB in size
PROCESSING_LOG = Path("../outputs/TUM_geojson_processing_log.txt")
BATCH_SIZE = 10000  # Process buildings in chunks to manage memory
# ----------------------------

GEOM_ERROR = object()


def _get_srid_via_fiona(path):
    try:
        with fiona.open(str(path), "r") as src:
            crs = src.crs
        if not crs:
            return None
        epsg = CRS.from_user_input(crs).to_epsg()
        return int(epsg) if epsg is not None else None
    except Exception:
        return None


def _detect_geom_col(con, table_name):
    try:
        df = con.execute(f"DESCRIBE {table_name}").df()
        for col in df["column_name"]:
            if col.lower() in ("geometry", "geom", "wkb_geometry", "geom_wkb", "shape"):
                return col
    except Exception:
        pass
    return None


def _is_file_processed(name):
    if not PROCESSING_LOG.exists():
        return False
    try:
        with open(PROCESSING_LOG, "r") as f:
            return name in {ln.strip() for ln in f if ln.strip()}
    except Exception:
        return False


def _mark_processed(name):
    # Ensure the outputs directory exists
    PROCESSING_LOG.parent.mkdir(exist_ok=True, parents=True)
    with open(PROCESSING_LOG, "a") as f:
        f.write(name + "\n")


def _is_stable(p: Path, wait=FILE_STABILITY_WAIT):
    try:
        s1 = p.stat()
        size1 = s1.st_size
        time.sleep(wait)
        s2 = p.stat()
        return size1 == s2.st_size and s2.st_size >= MIN_FILE_SIZE
    except Exception:
        return False


def load_census_once(con, census_path: Path, table_name: str):
    """Load census blocks into DuckDB (done once at startup)."""
    if not census_path.exists():
        return None
    try:
        print(f"Loading {census_path.name}...")
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM ST_Read('{census_path}')")

        geom_col = _detect_geom_col(con, table_name)
        if geom_col is None:
            print(f"  ✗ No geometry column in {census_path.name}")
            return None

        srid = _get_srid_via_fiona(census_path)
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  ✓ Loaded {count:,} census blocks (SRID: {srid})")
        return srid
    except Exception as e:
        print(f"  ✗ Failed to load {census_path.name}:", e)
        traceback.print_exc()
        return None


def process_tile(con, tile_path: Path, us_srid: int, canada_srid: int):
    """
    Process one building tile:
    - Join with US census blocks
    - Join with Canada census blocks
    - Return filtered buildings in OUTPUT_CRS
    """
    tmp_buildings = "buildings_tmp"

    try:
        # Get file size
        file_size_bytes = tile_path.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)  # Convert to MB
        file_size_gb = file_size_bytes / (1024 * 1024 * 1024)  # Convert to GB

        # Format size nicely
        if file_size_gb >= 1:
            size_str = f"{file_size_gb:.2f} GB"
        else:
            size_str = f"{file_size_mb:.2f} MB"

        print(f"\n{'=' * 60}")
        print(f"Processing: {tile_path.name} ({size_str})")
        print(f"{'=' * 60}")

        # Load buildings
        print("  Loading buildings into DuckDB...")
        con.execute(f"CREATE OR REPLACE TABLE {tmp_buildings} AS SELECT * FROM ST_Read('{tile_path}')")

        geom_col = _detect_geom_col(con, tmp_buildings)
        if geom_col is None:
            print("  ✗ No geometry column in buildings")
            con.execute(f"DROP TABLE IF EXISTS {tmp_buildings}")
            return GEOM_ERROR

        # Get building SRID
        try:
            b_srid = con.execute(f"SELECT ST_SRID({geom_col}) FROM {tmp_buildings} LIMIT 1").fetchone()[0]
            b_srid = int(b_srid)
        except Exception:
            b_srid = _get_srid_via_fiona(tile_path)
            if b_srid is None:
                print("  ✗ Could not detect building SRID")
                con.execute(f"DROP TABLE IF EXISTS {tmp_buildings}")
                return GEOM_ERROR

        total_buildings = con.execute(f"SELECT COUNT(*) FROM {tmp_buildings}").fetchone()[0]
        print(f"  Buildings in tile: {total_buildings:,} (SRID: {b_srid})")

        results = []

        # TRY US CENSUS
        if us_srid is not None:
            print(f"  Checking US census blocks...")
            sql = f"""
                SELECT b.* EXCLUDE {geom_col},
                       ST_AsWKB(ST_Transform(b.{geom_col}, 'EPSG:{b_srid}', '{OUTPUT_CRS}')) AS geom_wkb
                FROM {tmp_buildings} b
                INNER JOIN census_us c
                  ON ST_Intersects(
                      ST_Transform(b.{geom_col}, 'EPSG:{b_srid}', 'EPSG:{us_srid}'),
                      c.geom
                  )
            """

            try:
                us_matches = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
                if us_matches > 0:
                    print(f"    ✓ Found {us_matches:,} buildings in US school areas")
                    df = con.execute(sql).df()
                    results.append(df)
                else:
                    print(f"    No US matches")
            except Exception as e:
                print(f"    ✗ US join failed: {e}")

        # TRY CANADA CENSUS
        if canada_srid is not None:
            print(f"  Checking Canada census blocks...")
            sql = f"""
                SELECT b.* EXCLUDE {geom_col},
                       ST_AsWKB(ST_Transform(b.{geom_col}, 'EPSG:{b_srid}', '{OUTPUT_CRS}')) AS geom_wkb
                FROM {tmp_buildings} b
                INNER JOIN census_canada c
                  ON ST_Intersects(
                      ST_Transform(b.{geom_col}, 'EPSG:{b_srid}', 'EPSG:{canada_srid}'),
                      c.geom
                  )
            """

            try:
                ca_matches = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
                if ca_matches > 0:
                    print(f"    ✓ Found {ca_matches:,} buildings in Canada school areas")
                    df = con.execute(sql).df()
                    results.append(df)
                else:
                    print(f"    No Canada matches")
            except Exception as e:
                print(f"    ✗ Canada join failed: {e}")

        # Cleanup
        con.execute(f"DROP TABLE IF EXISTS {tmp_buildings}")

        if not results:
            print("  → No buildings in school areas")
            return None

        # Combine US + Canada results if both exist
        combined = pd.concat(results, ignore_index=True)

        # Convert WKB to GeoDataFrame
        from shapely import wkb
        combined['geometry'] = combined['geom_wkb'].apply(lambda x: wkb.loads(bytes(x)))
        combined = combined.drop('geom_wkb', axis=1)
        gdf = gpd.GeoDataFrame(combined, geometry='geometry', crs=OUTPUT_CRS)

        print(f"  ✓ Total filtered buildings: {len(gdf):,}")
        return gdf

    except Exception as e:
        print(f"  ✗ Error processing tile: {e}")
        traceback.print_exc()
        try:
            con.execute(f"DROP TABLE IF EXISTS {tmp_buildings}")
        except:
            pass
        return GEOM_ERROR


def append_to_gpkg(gdf: gpd.GeoDataFrame, output_path: Path):
    """Append buildings to the master GPKG (with error handling)."""
    try:
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.exists():
            # Read existing, concat, write back (safer but slower for large files)
            existing = gpd.read_file(output_path, layer="buildings")
            combined = pd.concat([existing, gdf], ignore_index=True)

            # Use proper .gpkg.tmp extension to avoid warning
            tmp_path = output_path.parent / f"{output_path.stem}_tmp.gpkg"
            combined.to_file(tmp_path, driver="GPKG", layer="buildings")
            os.replace(tmp_path, output_path)
            print(f"  ✓ Appended {len(gdf):,} buildings (total: {len(combined):,})")
        else:
            gdf.to_file(output_path, driver="GPKG", layer="buildings")
            print(f"  ✓ Created {output_path.name} with {len(gdf):,} buildings")
    except Exception as e:
        print(f"  ✗ Failed to write to GPKG: {e}")
        traceback.print_exc()

def main():
    # Connect to DuckDB
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA threads={N_THREADS};")
    except:
        pass

    # Load spatial extension
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
    except:
        try:
            con.execute("LOAD spatial;")
        except Exception as e:
            print("Failed to load DuckDB spatial extension:", e)
            return

    # Load census blocks ONCE at startup
    print("\n" + "=" * 60)
    print("LOADING CENSUS BLOCKS")
    print("=" * 60)
    us_srid = load_census_once(con, US_CENSUS, "census_us")
    canada_srid = load_census_once(con, CANADA_CENSUS, "census_canada")

    if us_srid is None and canada_srid is None:
        print("\n✗ No census blocks loaded - aborting")
        con.close()
        return

    # Process building tiles continuously
    print("\n" + "=" * 60)
    print("PROCESSING BUILDING TILES")
    print("=" * 60)

    processed_count = 0
    error_count = 0

    while True:
        # Find unprocessed files
        files = sorted(BUILDING_DIR.glob("*.geojson"))
        unprocessed = [f for f in files if not _is_file_processed(f.name)]

        if not unprocessed:
            print(f"\nNo new files to process. Waiting 15 seconds...")
            time.sleep(15)
            continue

        print(f"\nFound {len(unprocessed)} unprocessed files")

        for tile_path in unprocessed:
            # Check if file is stable (finished downloading)
            if not _is_stable(tile_path):
                print(f"\n⏳ {tile_path.name} - still downloading, skipping for now")
                continue

            # Process the tile
            result = process_tile(con, tile_path, us_srid, canada_srid)

            if result is GEOM_ERROR:
                print(f"  ✗ Geometry error - keeping file for inspection")
                _mark_processed(tile_path.name)
                error_count += 1
                continue

            if result is None:
                print(f"  → No matches - marking as processed and deleting file")
                _mark_processed(tile_path.name)
                processed_count += 1  # ADD THIS LINE
                # Delete since no errors, just no matches
                try:
                    tile_path.unlink()
                    print(f"  🗑️  Deleted {tile_path.name}")
                except Exception as e:
                    print(f"  ⚠️  Could not delete: {e}")

                print(f"\n📊 Progress: {processed_count} tiles processed, {error_count} errors")  # ADD THIS LINE
                continue

            # Append to master GPKG
            append_to_gpkg(result, OUTPUT_GPKG)
            _mark_processed(tile_path.name)
            processed_count += 1

            # DELETE THE ORIGINAL FILE (successful processing, no errors)
            try:
                tile_path.unlink()
                print(f"  🗑️  Deleted {tile_path.name}")
            except Exception as e:
                print(f"  ⚠️  Could not delete {tile_path.name}: {e}")

            print(f"\n📊 Progress: {processed_count} tiles processed, {error_count} errors")

        time.sleep(60)  # Wait before checking for new files

    con.close()


if __name__ == "__main__":
    main()