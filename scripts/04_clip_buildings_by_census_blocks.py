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
from pyproj import CRS
import geopandas as gpd
import pandas as pd
import fiona
from shapely.geometry import mapping

# ---------- CONFIG ----------
BUILDING_DIR = Path("../building_data/LoD1/northamerica")
US_CENSUS = Path("../inputs/census_blocks/united_states/us_census_blocks_with_schools.gpkg")
CANADA_CENSUS = Path("../inputs/census_blocks/canada/Canada_census_blocks_with_schools.gpkg")

OUTPUT_GPKG = Path("../outputs/buildings_near_schools_backup.gpkg")
OUTPUT_EPSG = 3857
OUTPUT_CRS = f"EPSG:{OUTPUT_EPSG}"
N_THREADS = 4
FILE_STABILITY_WAIT = 15 # 15 second wait time to check file stability
MIN_FILE_SIZE = 1000 # Processes files at least 1 MB in size
PROCESSING_LOG = Path("../outputs/text/TUM_geojson_processing_log.txt")
BATCH_SIZE = 10000  # Process buildings in chunks to manage memory
DELETE_PROCESSED_FILES = False  # Set to True to delete files after processing

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


def load_census_once(con, census_path: Path, table_name: str, target_epsg:int):
    """Load census blocks into DuckDB and transform to OUTPUT_CRS (done once at startup)."""
    if not census_path.exists():
        return None
    try:
        print(f"Loading {census_path.name}...")
        # Create a temp table from read then transform geometry column to OUTPUT_CRS
        con.execute(f"CREATE OR REPLACE TABLE {table_name}_raw AS SELECT * FROM ST_Read('{census_path}')")
        geom_col = _detect_geom_col(con, f"{table_name}_raw")
        if geom_col is None:
            print(f"  ✗ No geometry column in {census_path.name}")
            con.execute(f"DROP TABLE IF EXISTS {table_name}_raw")
            return None

        # detect source srid
        src_srid = _get_srid_via_fiona(census_path)
        if src_srid is None:
            print(f"  ✗ Could not detect SRID for {census_path.name}")
            con.execute(f"DROP TABLE IF EXISTS {table_name}_raw")
            return None

        # Create a transformed table in OUTPUT_CRS (done once)
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * EXCLUDE {geom_col},
                   ST_Transform({geom_col}, 'EPSG:{src_srid}', 'EPSG:{target_epsg}') AS geom
            FROM {table_name}_raw
        """)
        con.execute(f"DROP TABLE IF EXISTS {table_name}_raw")

        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  ✓ Loaded & transformed {count:,} census blocks (from SRID {src_srid} -> EPSG:{target_epsg})")
        return src_srid
    except Exception as e:
        print(f"  ✗ Failed to load {census_path.name}:", e)
        traceback.print_exc()
        try:
            con.execute(f"DROP TABLE IF EXISTS {table_name}_raw")
        except:
            pass
        return None


def process_tile(con, tile_path: Path, us_srid: int, canada_srid: int):
    """
    Process one building tile:
    - Join with US census blocks
    - Join with Canada census blocks
    - Return filtered buildings in OUTPUT_CRS (EPSG int stored in OUTPUT_EPSG)

    Notes:
    - Avoids ST_SetSRID (compatibility issue with your DuckDB build).
    - Heuristically detects when a declared EPSG:4326 file actually contains projected meters,
      and in that case *does not* reproject (uses the table as-is).
    """
    tmp_buildings = "buildings_tmp"

    try:
        # Get file size (for logging)
        file_size_bytes = tile_path.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        file_size_gb = file_size_bytes / (1024 * 1024 * 1024)
        size_str = f"{file_size_gb:.2f} GB" if file_size_gb >= 1 else f"{file_size_mb:.2f} MB"

        print(f"\n{'=' * 60}")
        print(f"Processing: {tile_path.name} ({size_str})")
        print(f"{'=' * 60}")

        # Load buildings into DuckDB
        print("  Loading buildings into DuckDB...")
        con.execute(f"CREATE OR REPLACE TABLE {tmp_buildings} AS SELECT * FROM ST_Read('{tile_path}')")

        geom_col = _detect_geom_col(con, tmp_buildings)
        if geom_col is None:
            print("  ✗ No geometry column in buildings")
            con.execute(f"DROP TABLE IF EXISTS {tmp_buildings}")
            return GEOM_ERROR

        # Get building SRID (try SQL first, fallback to Fiona)
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

        # -------------------------
        # Heuristic: are the coordinates already projected (meters)?
        # We'll inspect a quick numeric extent on X/Y to see if values look like degrees (~ -180..180 / -90..90)
        # or meters (large magnitude, e.g. |x| > 1e5).
        # This uses ST_X and ST_Y aggregates (fast) if available. If the aggregate fails, fall back to sampling WKB.
        # -------------------------
        coords_look_projected = False
        try:
            # sample up to 1000 geometries for min/max X/Y (fast aggregate on a subquery)
            extent_sql = f"""
                SELECT
                  MIN(ST_X(b.{geom_col})) AS xmin,
                  MAX(ST_X(b.{geom_col})) AS xmax,
                  MIN(ST_Y(b.{geom_col})) AS ymin,
                  MAX(ST_Y(b.{geom_col})) AS ymax
                FROM (SELECT {geom_col} FROM {tmp_buildings} LIMIT 1000) b
            """
            row = con.execute(extent_sql).fetchone()
            xmin, xmax, ymin, ymax = row
            # if any coordinate magnitude is large (>100000) assume projected meters
            if xmin is not None and xmax is not None and ymin is not None and ymax is not None:
                if (abs(xmin) > 1e5 or abs(xmax) > 1e5 or abs(ymin) > 1e5 or abs(ymax) > 1e5):
                    coords_look_projected = True
                else:
                    coords_look_projected = False
            else:
                # fallback to WKB-sampling below if nulls
                raise Exception("extent nulls - fallback sample")
        except Exception:
            # fallback: sample some WKBs and inspect numeric ranges in Python (safe but slightly slower)
            try:
                sample_sql = f"SELECT ST_AsWKB({geom_col}) AS wkb FROM {tmp_buildings} LIMIT 200"
                df_sample = con.execute(sample_sql).df()
                from shapely import wkb as _wkb
                xs = []
                ys = []
                for w in df_sample['wkb'].dropna():
                    try:
                        geom = _wkb.loads(bytes(w)) if not isinstance(w, memoryview) else _wkb.loads(w.tobytes())
                        # take bounds
                        minx, miny, maxx, maxy = geom.bounds
                        xs.extend([minx, maxx])
                        ys.extend([miny, maxy])
                    except Exception:
                        continue
                if xs and ys and (max(map(abs, xs)) > 1e5 or max(map(abs, ys)) > 1e5):
                    coords_look_projected = True
                else:
                    coords_look_projected = False
            except Exception:
                # as a safe default, assume the declared SRID is correct (i.e. do transform)
                coords_look_projected = False

        # -------------------------
        # Build transformed table (if needed). Important: we DO NOT call ST_SetSRID anywhere.
        # Cases:
        #  - declared SRID == OUTPUT_EPSG -> use tmp_buildings directly
        #  - declared SRID == 4326 but coords look projected -> don't transform; use tmp_buildings
        #  - declared SRID != OUTPUT_EPSG -> normal ST_Transform(tmp_geom, 'EPSG:{b_srid}', 'EPSG:{OUTPUT_EPSG}')
        # -------------------------
        buildings_for_join = tmp_buildings
        transformed_table = None

        if b_srid == OUTPUT_EPSG:
            # already in output CRS
            buildings_for_join = tmp_buildings
        elif b_srid == 4326 and coords_look_projected:
            # declared 4326 but coords are meters -> treat as already in output CRS (no transform)
            print("  ⚠️  Heuristic: coordinates look projected (meters). Will NOT reproject from declared SRID.")
            buildings_for_join = tmp_buildings
        else:
            # create a transformed temp table (one-shot)
            transformed_table = f"{tmp_buildings}_xform"
            try:
                con.execute(f"DROP TABLE IF EXISTS {transformed_table}")
                con.execute(f"""
                    CREATE OR REPLACE TABLE {transformed_table} AS
                    SELECT * EXCLUDE {geom_col},
                           ST_Transform({geom_col}, 'EPSG:{b_srid}', 'EPSG:{OUTPUT_EPSG}') AS geom
                    FROM {tmp_buildings}
                """)
                buildings_for_join = transformed_table
                print(f"  ✓ Created transformed table `{transformed_table}` (SRID {b_srid} -> EPSG:{OUTPUT_EPSG})")
            except Exception as e:
                print(f"  ✗ Failed to create transformed buildings table: {e}")
                traceback.print_exc()
                # fall back to using original table (will likely cause join SRID mismatch)
                buildings_for_join = tmp_buildings

        # Which geometry column to use for joins (transformed uses "geom")
        join_geom = "geom" if buildings_for_join != tmp_buildings else geom_col
        print(f"  Using buildings table `{buildings_for_join}` with geometry column `{join_geom}`")

        # Now do the joins (US and Canada). census_* tables already transformed to OUTPUT_EPSG.
        if us_srid is not None:
            print(f"  Checking US census blocks...")
            sql_us = f"""
                SELECT b.* EXCLUDE {join_geom},
                       ST_AsWKB(b.{join_geom}) AS geom_wkb
                FROM {buildings_for_join} b
                INNER JOIN census_us c
                  ON ST_Intersects(b.{join_geom}, c.geom)
            """
            try:
                us_matches = con.execute(f"SELECT COUNT(*) FROM ({sql_us})").fetchone()[0]
                if us_matches > 0:
                    print(f"    ✓ Found {us_matches:,} buildings in US school areas")
                    df = con.execute(sql_us).df()
                    results.append(df)
                else:
                    print(f"    No US matches")
            except Exception as e:
                print(f"    ✗ US join failed: {e}")
                traceback.print_exc()

        if canada_srid is not None:
            print(f"  Checking Canada census blocks...")
            sql_ca = f"""
                SELECT b.* EXCLUDE {join_geom},
                       ST_AsWKB(b.{join_geom}) AS geom_wkb
                FROM {buildings_for_join} b
                INNER JOIN census_canada c
                  ON ST_Intersects(b.{join_geom}, c.geom)
            """
            try:
                ca_matches = con.execute(f"SELECT COUNT(*) FROM ({sql_ca})").fetchone()[0]
                if ca_matches > 0:
                    print(f"    ✓ Found {ca_matches:,} buildings in Canada school areas")
                    df = con.execute(sql_ca).df()
                    results.append(df)
                else:
                    print(f"    No Canada matches")
            except Exception as e:
                print(f"    ✗ Canada join failed: {e}")
                traceback.print_exc()

        # Cleanup temp tables
        try:
            con.execute(f"DROP TABLE IF EXISTS {tmp_buildings}")
        except:
            pass

        if transformed_table:
            try:
                con.execute(f"DROP TABLE IF EXISTS {transformed_table}")
            except:
                pass

        if not results:
            print("  → No buildings in school areas")
            return None

        # Combine results and convert WKB -> geometry safely (handle nulls)
        combined = pd.concat(results, ignore_index=True)
        from shapely import wkb as _wkb
        def _wkb_to_geom(val):
            try:
                if val is None:
                    return None
                if isinstance(val, memoryview):
                    val = val.tobytes()
                return _wkb.loads(bytes(val))
            except Exception:
                return None

        combined['geometry'] = combined['geom_wkb'].apply(_wkb_to_geom)
        combined = combined.drop(columns=['geom_wkb'], errors='ignore')
        gdf = gpd.GeoDataFrame(combined, geometry='geometry', crs=OUTPUT_CRS)
        gdf = gdf[~gdf.geometry.isna()]

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


def _pandas_to_fiona_props(df: pd.DataFrame):
    """Minimal, portable dtype -> Fiona property type mapping (no width suffixes)."""
    props = {}
    for col, dtype in df.dtypes.items():
        if col == "geometry":
            continue
        if pd.api.types.is_integer_dtype(dtype):
            props[col] = 'int'
        elif pd.api.types.is_float_dtype(dtype):
            props[col] = 'float'
        elif pd.api.types.is_bool_dtype(dtype):
            props[col] = 'bool'
        elif pd.api.types.is_datetime64_dtype(dtype):
            props[col] = 'datetime'
        else:
            props[col] = 'str'
    return props


def append_to_gpkg(gdf: gpd.GeoDataFrame, output_path: Path, layer_name="buildings"):
    """Append buildings to the master GPKG (in-place append via Fiona)."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Build Fiona schema (portable types)
        props = _pandas_to_fiona_props(gdf)
        geom_types = list(gdf.geom_type.unique())
        geom_type = geom_types[0] if len(geom_types) == 1 else "GeometryCollection"
        schema = {"geometry": geom_type, "properties": props}

        # Prepare records (filter out missing/empty geometries)
        def row_to_record(row):
            geom = row.geometry
            if geom is None or getattr(geom, "is_empty", False):
                return None
            props_dict = row.drop(labels='geometry').to_dict()
            props_dict = {k: (None if pd.isna(v) else v) for k, v in props_dict.items()}
            return {"geometry": mapping(geom), "properties": props_dict}

        records = []
        for _, row in gdf.iterrows():
            rec = row_to_record(row)
            if rec is not None:
                records.append(rec)

        # Create layer if missing
        if not output_path.exists():
            with fiona.open(
                    str(output_path),
                    mode='w',
                    driver='GPKG',
                    layer=layer_name,
                    schema=schema,
                    crs=CRS.from_epsg(OUTPUT_EPSG).to_wkt("WKT1_GDAL")
            ) as dst:
                if records:
                    dst.writerecords(records)
            print(f"  ✓ Created {output_path.name} with {len(records):,} buildings")
            return

        # When appending, align columns with existing schema
        with fiona.open(str(output_path), layer=layer_name, mode='r') as src:
            existing_schema = src.schema
            existing_props = existing_schema['properties']

        # Add missing columns to gdf (fill with None)
        for prop_name in existing_props.keys():
            if prop_name not in gdf.columns:
                gdf[prop_name] = None

        # Drop columns not in schema (or warn user)
        extra_cols = set(gdf.columns) - set(existing_props.keys()) - {'geometry'}
        if extra_cols:
            print(f"  ⚠️  Warning: Dropping columns not in schema: {extra_cols}")
            gdf = gdf.drop(columns=list(extra_cols))

        # Build records aligned to existing schema (only iterate ONCE)
        records = []
        for _, row in gdf.iterrows():
            geom = row.geometry
            if geom is None or getattr(geom, "is_empty", False):
                continue
            props_dict = row.drop(labels='geometry').to_dict()
            props_dict = {k: (None if pd.isna(v) else v) for k, v in props_dict.items()}
            records.append({"geometry": mapping(geom), "properties": props_dict})

        # Append in place
        with fiona.open(str(output_path), layer=layer_name, mode='a') as dst:
            if records:
                dst.writerecords(records)

        print(f"  ✓ Appended {len(records):,} buildings")

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
    us_srid = load_census_once(con, US_CENSUS, "census_us", OUTPUT_EPSG)
    canada_srid = load_census_once(con, CANADA_CENSUS, "census_canada", OUTPUT_EPSG)

    if us_srid is not None:
        print(f"\n  census_us rows: {con.execute('SELECT COUNT(*) FROM census_us').fetchone()[0]}")
    if canada_srid is not None:
        print(f"  census_canada rows: {con.execute('SELECT COUNT(*) FROM census_canada').fetchone()[0]}")

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
                processed_count += 1
                # Delete since no errors, just no matches
                if DELETE_PROCESSED_FILES:
                    try:
                        tile_path.unlink()
                        print(f"  🗑️  Deleted {tile_path.name}")
                    except Exception as e:
                        print(f"  ⚠️  Could not delete: {e}")
                else:
                    print(f"  📁 Keeping {tile_path.name} (DELETE_PROCESSED_FILES=False)")

                print(f"\n📊 Progress: {processed_count} tiles processed, {error_count} errors")
                continue

            # Append to master GPKG
            append_to_gpkg(result, OUTPUT_GPKG)
            _mark_processed(tile_path.name)
            processed_count += 1

            # DELETE THE ORIGINAL FILE (successful processing, no errors)
            if DELETE_PROCESSED_FILES:
                try:
                    tile_path.unlink()
                    print(f"  🗑️  Deleted {tile_path.name}")
                except Exception as e:
                    print(f"  ⚠️  Could not delete {tile_path.name}: {e}")
            else:
                print(f"  📁 Keeping {tile_path.name} (DELETE_PROCESSED_FILES=False)")

            print(f"\n📊 Progress: {processed_count} tiles processed, {error_count} errors")

        time.sleep(60)  # Wait before checking for new files

    con.close()


if __name__ == "__main__":
    main()