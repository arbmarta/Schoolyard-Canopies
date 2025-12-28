import geopandas as gpd
from pathlib import Path
import duckdb
from shapely import wkb
import pandas as pd
import fiona
from pyproj import CRS

# ============================================================================
# CONFIGURATION
# ============================================================================

COUNTRIES = {
    'canada': {
        'schools_path': '../inputs/schools/canada/canada_school_points.gpkg',
        'schools_layer': 'schools',
        'buildings_path': '../outputs/buildings_near_schools.gpkg',
        'buildings_layer': 'buildings',
        'output_path': '../outputs/canada_school_footprints.gpkg',
        'epsg': 3347,
        'crs': 'EPSG:3347'
    },
    'united_states': {
        'schools_path': '../inputs/schools/united_states/US_school_points.gpkg',
        'schools_layer': 'schools',
        'buildings_path': '../outputs/buildings_near_schools.gpkg',
        'buildings_layer': 'buildings',
        'output_path': '../outputs/us_school_footprints.gpkg',
        'epsg': 5070,
        'crs': 'EPSG:5070'
    }
}

N_THREADS = 4


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _detect_geom_col(con, table_name):
    """Detect geometry column name in a DuckDB table."""
    try:
        df = con.execute(f"DESCRIBE {table_name}").df()
        for col in df["column_name"]:
            if col.lower() in ("geometry", "geom", "wkb_geometry", "geom_wkb", "shape"):
                return col
    except Exception:
        pass
    return None


def _get_srid_via_fiona(path, layer=None):
    """Get SRID from a geospatial file using fiona."""
    try:
        with fiona.open(str(path), "r", layer=layer) as src:
            crs = src.crs
        if not crs:
            return None
        epsg = CRS.from_user_input(crs).to_epsg()
        return int(epsg) if epsg is not None else None
    except Exception:
        return None


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_country(country_name, config):
    """Identify school footprints for a country using DuckDB."""

    print("=" * 60)
    print(f"{country_name.upper()}: School Building Footprint Identification")
    print(f"Using {config['crs']} with DuckDB")
    print("=" * 60)

    # ========================================================================
    # Initialize DuckDB
    # ========================================================================
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA threads={N_THREADS};")
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
    except:
        try:
            con.execute("LOAD spatial;")
        except Exception as e:
            print(f"Failed to load DuckDB spatial extension: {e}")
            return False

    # ========================================================================
    # PART 1: Load school points into DuckDB
    # ========================================================================
    print("\n[PART 1] Loading school points...")

    schools_path = Path(config['schools_path'])
    if not schools_path.exists():
        print(f"ERROR: School points not found at {schools_path}")
        con.close()
        return False

    try:
        con.execute(
            f"CREATE OR REPLACE TABLE schools AS SELECT * FROM ST_Read('{schools_path}', layer='{config['schools_layer']}')")
        school_geom_col = _detect_geom_col(con, "schools")

        if school_geom_col is None:
            print("ERROR: No geometry column in schools")
            con.close()
            return False

        school_count = con.execute("SELECT COUNT(*) FROM schools").fetchone()[0]

        # Get SRID from file metadata instead of using ST_SRID
        school_srid = _get_srid_via_fiona(schools_path, layer=config['schools_layer'])
        if school_srid is None:
            print("ERROR: Could not detect school SRID")
            con.close()
            return False

        print(f"Loaded {school_count:,} school points")
        print(f"Geometry column: {school_geom_col}")
        print(f"SRID: {school_srid}")

    except Exception as e:
        print(f"ERROR loading schools: {e}")
        con.close()
        return False

    # ========================================================================
    # PART 2: Load buildings into DuckDB
    # ========================================================================
    print("\n[PART 2] Loading buildings...")

    buildings_path = Path(config['buildings_path'])
    if not buildings_path.exists():
        print(f"ERROR: Buildings file not found at {buildings_path}")
        con.close()
        return False

    try:
        # Try with layer, fall back to no layer
        try:
            con.execute(
                f"CREATE OR REPLACE TABLE buildings AS SELECT * FROM ST_Read('{buildings_path}', layer='{config['buildings_layer']}')")
            building_layer = config['buildings_layer']
        except:
            con.execute(f"CREATE OR REPLACE TABLE buildings AS SELECT * FROM ST_Read('{buildings_path}')")
            building_layer = None

        building_geom_col = _detect_geom_col(con, "buildings")

        if building_geom_col is None:
            print("ERROR: No geometry column in buildings")
            con.close()
            return False

        building_count = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]

        # Get SRID from file metadata
        building_srid = _get_srid_via_fiona(buildings_path, layer=building_layer)
        if building_srid is None:
            print("ERROR: Could not detect building SRID")
            con.close()
            return False

        print(f"Loaded {building_count:,} buildings")
        print(f"Geometry column: {building_geom_col}")
        print(f"SRID: {building_srid}")

    except Exception as e:
        print(f"ERROR loading buildings: {e}")
        con.close()
        return False

    # ========================================================================
    # PART 3: Perform spatial intersection
    # ========================================================================
    print("\n[PART 3] Performing spatial intersection...")

    target_srid = config['epsg']
    target_crs = config['crs']

    # Build spatial join query with proper SRID transformations
    sql = f"""
    SELECT 
        b.* EXCLUDE {building_geom_col},
        ST_AsWKB(ST_Transform(b.{building_geom_col}, 'EPSG:{building_srid}', '{target_crs}')) AS geom_wkb
    FROM buildings b
    INNER JOIN schools s
    ON ST_Intersects(
        ST_Transform(b.{building_geom_col}, 'EPSG:{building_srid}', 'EPSG:{target_srid}'),
        ST_Transform(s.{school_geom_col}, 'EPSG:{school_srid}', 'EPSG:{target_srid}')
    )
    """

    try:
        print("  Running spatial join query...")
        result_df = con.execute(sql).df()

        if len(result_df) == 0:
            print("  No buildings intersect school points!")
            con.close()
            return False

        print(f"  Found {len(result_df):,} school-building intersections")

        # Convert WKB to geometry
        print("  Converting WKB to geometries...")
        result_df['geometry'] = result_df['geom_wkb'].apply(lambda x: wkb.loads(bytes(x)))
        result_df = result_df.drop('geom_wkb', axis=1)

        school_footprints = gpd.GeoDataFrame(result_df, geometry='geometry', crs=target_crs)

        # Remove duplicate buildings (if a building intersects multiple schools)
        initial_count = len(school_footprints)
        school_footprints = school_footprints.drop_duplicates(subset=['geometry'])
        final_count = len(school_footprints)

        if initial_count > final_count:
            print(f"  Removed {initial_count - final_count} duplicate buildings")

        print(f"\nUnique buildings containing schools: {final_count:,}")
        print(f"  Total buildings analyzed: {building_count:,}")
        print(f"  Percentage with schools: {final_count / building_count * 100:.2f}%")

    except Exception as e:
        print(f"ERROR during spatial join: {e}")
        import traceback
        traceback.print_exc()
        con.close()
        return False

    # ========================================================================
    # PART 4: Calculate unmatched schools
    # ========================================================================
    print("\n[PART 4] Calculating unmatched schools...")

    try:
        # Count schools that intersect buildings
        matched_schools_sql = f"""
        SELECT COUNT(DISTINCT s.rowid) as matched_count
        FROM schools s
        INNER JOIN buildings b
        ON ST_Intersects(
            ST_Transform(s.{school_geom_col}, 'EPSG:{school_srid}', 'EPSG:{target_srid}'),
            ST_Transform(b.{building_geom_col}, 'EPSG:{building_srid}', 'EPSG:{target_srid}')
        )
        """

        matched_count = con.execute(matched_schools_sql).fetchone()[0]
        unmatched_count = school_count - matched_count

        print(
            f"Schools WITH building footprints: {matched_count:,}/{school_count:,} ({matched_count / school_count * 100:.2f}%)")
        print(
            f"Schools WITHOUT building footprints: {unmatched_count:,}/{school_count:,} ({unmatched_count / school_count * 100:.2f}%)")

        # Get unmatched schools for export
        unmatched_sql = f"""
        SELECT 
            s.* EXCLUDE {school_geom_col},
            ST_AsWKB(ST_Transform(s.{school_geom_col}, 'EPSG:{school_srid}', '{target_crs}')) AS geom_wkb
        FROM schools s
        WHERE s.rowid NOT IN (
            SELECT DISTINCT s2.rowid
            FROM schools s2
            INNER JOIN buildings b
            ON ST_Intersects(
                ST_Transform(s2.{school_geom_col}, 'EPSG:{school_srid}', 'EPSG:{target_srid}'),
                ST_Transform(b.{building_geom_col}, 'EPSG:{building_srid}', 'EPSG:{target_srid}')
            )
        )
        """

        unmatched_df = con.execute(unmatched_sql).df()

        if len(unmatched_df) > 0:
            unmatched_df['geometry'] = unmatched_df['geom_wkb'].apply(lambda x: wkb.loads(bytes(x)))
            unmatched_df = unmatched_df.drop('geom_wkb', axis=1)
            unmatched_schools = gpd.GeoDataFrame(unmatched_df, geometry='geometry', crs=target_crs)
        else:
            unmatched_schools = None

    except Exception as e:
        print(f"Warning: Could not calculate unmatched schools: {e}")
        unmatched_schools = None

    con.close()

    # ========================================================================
    # PART 5: Save outputs
    # ========================================================================
    print("\n[PART 5] Saving outputs...")

    output_path = Path(config['output_path'])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save school footprints
    school_footprints.to_file(output_path, driver='GPKG', layer='school_buildings')
    print(f"Saved school footprints to: {output_path}")

    # Save unmatched schools as CSV and GeoPackage
    if unmatched_schools is not None and len(unmatched_schools) > 0:
        # Save as GeoPackage
        unmatched_path = output_path.parent / f"schools_without_buildings_{country_name.lower().replace(' ', '_')}.gpkg"
        unmatched_schools.to_file(unmatched_path, driver='GPKG', layer='unmatched_schools')
        print(f"Saved unmatched schools (GPKG) to: {unmatched_path}")

        # Save as CSV (drop geometry, keep all other attributes)
        csv_path = output_path.parent / f"schools_without_buildings_{country_name.lower().replace(' ', '_')}.csv"
        unmatched_csv = unmatched_schools.drop(columns=['geometry'])
        unmatched_csv.to_csv(csv_path, index=False)
        print(f"Saved unmatched schools (CSV) to: {csv_path}")
        print(f"  CSV contains {len(unmatched_csv.columns)} columns and {len(unmatched_csv):,} rows")

    print(f"\n{'=' * 60}")
    print(f"SUCCESS!")
    print(f"{'=' * 60}")
    print(f"School footprints: {len(school_footprints):,}")
    print(f"Unmatched schools: {unmatched_count:,}")
    print(f"CRS: {target_crs}")
    print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")
    print(f"{'=' * 60}\n")

    return True


# ============================================================================
# RUN
# ============================================================================

if __name__ == '__main__':
    # Process Canada
    process_country('canada', COUNTRIES['canada'])

    # Process United States
    process_country('united_states', COUNTRIES['united_states'])