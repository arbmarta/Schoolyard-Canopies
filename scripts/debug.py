import geopandas as gpd
from pathlib import Path
import duckdb
from shapely import wkb
from shapely.geometry import Point
import pandas as pd
import fiona
from pyproj import CRS
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
from typing import Optional, Tuple

# ============================================================================
# CONFIGURATION
# ============================================================================

COUNTRIES = {
    'canada': {
        'schools_path': '../inputs/schools/canada/canada_school_points.gpkg',
        'schools_layer': 'schools',
        'buildings_path': '../outputs/buildings_near_schools_backup.gpkg',
        'buildings_layer': 'buildings',
        'output_path': '../outputs/canada_school_footprints.gpkg',
        'epsg': 3347,
        'crs': 'EPSG:3347',
        'country_code': 'ca'
    },
    'united_states': {
        'schools_path': '../inputs/schools/united_states/US_school_points.gpkg',
        'schools_layer': 'schools',
        'buildings_path': '../outputs/buildings_near_schools_backup.gpkg',
        'buildings_layer': 'buildings',
        'output_path': '../outputs/us_school_footprints.gpkg',
        'epsg': 5070,
        'crs': 'EPSG:5070',
        'country_code': 'us'
    }
}

N_THREADS = 4

# OSM Geocoding settings
OSM_USER_AGENT = "school_footprint_matcher/1.0"
OSM_TIMEOUT = 10
OSM_DELAY = 1.0  # Delay between requests (seconds) - be respectful to OSM servers
MAX_GEOCODE_ATTEMPTS = 3


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


def build_address_string(row, address_fields=None):
    """
    Build an address string from school data.

    Common address field names to look for:
    - address, street, street_address, addr_street
    - city, municipality, town
    - state, province, region
    - zip, postal_code, postcode
    - name, school_name
    """
    if address_fields is None:
        # Try to detect common address fields
        address_fields = {
            'name': None,
            'street': None,
            'city': None,
            'state': None,
            'postal': None
        }

        columns = [c.lower() for c in row.index]

        # Detect name field
        for field in ['name', 'school_name', 'facility_name', 'schoolname']:
            if field in columns:
                address_fields['name'] = row.index[columns.index(field)]
                break

        # Detect street field
        for field in ['address', 'street', 'street_address', 'addr_street', 'full_address']:
            if field in columns:
                address_fields['street'] = row.index[columns.index(field)]
                break

        # Detect city field
        for field in ['city', 'municipality', 'town', 'locality']:
            if field in columns:
                address_fields['city'] = row.index[columns.index(field)]
                break

        # Detect state/province field
        for field in ['state', 'province', 'region', 'state_province']:
            if field in columns:
                address_fields['state'] = row.index[columns.index(field)]
                break

        # Detect postal code field
        for field in ['zip', 'zipcode', 'postal_code', 'postcode', 'postal']:
            if field in columns:
                address_fields['postal'] = row.index[columns.index(field)]
                break

    # Build address string
    parts = []

    if address_fields['name'] and pd.notna(row.get(address_fields['name'])):
        parts.append(str(row[address_fields['name']]))

    if address_fields['street'] and pd.notna(row.get(address_fields['street'])):
        parts.append(str(row[address_fields['street']]))

    if address_fields['city'] and pd.notna(row.get(address_fields['city'])):
        parts.append(str(row[address_fields['city']]))

    if address_fields['state'] and pd.notna(row.get(address_fields['state'])):
        parts.append(str(row[address_fields['state']]))

    if address_fields['postal'] and pd.notna(row.get(address_fields['postal'])):
        parts.append(str(row[address_fields['postal']]))

    return ', '.join(parts) if parts else None


def geocode_with_osm(address: str, country_code: str, geolocator: Nominatim) -> Optional[Tuple[float, float]]:
    """
    Geocode an address using OSM Nominatim.

    Returns:
        Tuple of (latitude, longitude) or None if geocoding fails
    """
    if not address or pd.isna(address):
        return None

    for attempt in range(MAX_GEOCODE_ATTEMPTS):
        try:
            location = geolocator.geocode(
                address,
                country_codes=country_code,
                timeout=OSM_TIMEOUT
            )

            if location:
                return (location.latitude, location.longitude)
            else:
                return None

        except GeocoderTimedOut:
            if attempt < MAX_GEOCODE_ATTEMPTS - 1:
                time.sleep(OSM_DELAY * 2)
                continue
            else:
                return None
        except GeocoderServiceError as e:
            print(f"    Geocoding service error: {e}")
            return None
        except Exception as e:
            print(f"    Unexpected geocoding error: {e}")
            return None

    return None


def geocode_unmatched_schools(unmatched_schools: gpd.GeoDataFrame, country_code: str) -> gpd.GeoDataFrame:
    """
    Attempt to geocode unmatched schools using OSM Nominatim.

    Returns:
        GeoDataFrame with additional columns: osm_lat, osm_lon, osm_geocoded, osm_address_used
    """
    print("\n[OSM GEOCODING] Starting geocoding of unmatched schools...")
    print(f"  Schools to geocode: {len(unmatched_schools):,}")
    print(f"  Delay between requests: {OSM_DELAY}s")
    print("  This may take a while - please be patient!")

    geolocator = Nominatim(user_agent=OSM_USER_AGENT)

    # Add new columns
    unmatched_schools['osm_lat'] = None
    unmatched_schools['osm_lon'] = None
    unmatched_schools['osm_geocoded'] = False
    unmatched_schools['osm_address_used'] = None

    success_count = 0

    for idx, row in unmatched_schools.iterrows():
        if (idx + 1) % 10 == 0:
            print(f"  Progress: {idx + 1}/{len(unmatched_schools)} ({success_count} successful)")

        # Build address string
        address = build_address_string(row)

        if not address:
            continue

        unmatched_schools.at[idx, 'osm_address_used'] = address

        # Geocode
        coords = geocode_with_osm(address, country_code, geolocator)

        if coords:
            unmatched_schools.at[idx, 'osm_lat'] = coords[0]
            unmatched_schools.at[idx, 'osm_lon'] = coords[1]
            unmatched_schools.at[idx, 'osm_geocoded'] = True
            success_count += 1

        # Be respectful to OSM servers
        time.sleep(OSM_DELAY)

    print(f"\n  Geocoding complete!")
    print(
        f"  Successfully geocoded: {success_count}/{len(unmatched_schools)} ({success_count / len(unmatched_schools) * 100:.1f}%)")

    return unmatched_schools


def retry_matching_with_osm_coords(
        geocoded_schools: gpd.GeoDataFrame,
        con: duckdb.DuckDBPyConnection,
        building_geom_col: str,
        building_srid: int,
        target_srid: int,
        target_crs: str
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Retry matching schools using OSM-geocoded coordinates.

    Returns:
        Tuple of (newly_matched_schools, still_unmatched_schools)
    """
    print("\n[OSM RETRY] Retrying building matching with OSM coordinates...")

    # Filter to only successfully geocoded schools
    geocoded = geocoded_schools[geocoded_schools['osm_geocoded'] == True].copy()

    if len(geocoded) == 0:
        print("  No successfully geocoded schools to retry")
        return gpd.GeoDataFrame(), geocoded_schools

    print(f"  Retrying {len(geocoded):,} schools with OSM coordinates")

    # Create new geometries from OSM coordinates (in WGS84/EPSG:4326)
    geocoded['osm_geometry'] = geocoded.apply(
        lambda row: Point(row['osm_lon'], row['osm_lat']),
        axis=1
    )

    # Create temporary GeoDataFrame with OSM geometries
    osm_schools = gpd.GeoDataFrame(
        geocoded,
        geometry='osm_geometry',
        crs='EPSG:4326'
    )

    # Load OSM schools into DuckDB
    temp_gpkg = Path('/tmp/osm_schools_temp.gpkg')
    osm_schools.to_file(temp_gpkg, driver='GPKG', layer='osm_schools')

    con.execute(f"CREATE OR REPLACE TABLE osm_schools AS SELECT * FROM ST_Read('{temp_gpkg}', layer='osm_schools')")
    osm_geom_col = _detect_geom_col(con, "osm_schools")

    # Perform spatial intersection with buildings
    sql = f"""
    SELECT 
        b.* EXCLUDE {building_geom_col},
        s.* EXCLUDE {osm_geom_col},
        ST_AsWKB(ST_Transform(b.{building_geom_col}, 'EPSG:{building_srid}', '{target_crs}')) AS geom_wkb
    FROM buildings b
    INNER JOIN osm_schools s
    ON ST_Intersects(
        ST_Transform(b.{building_geom_col}, 'EPSG:{building_srid}', 'EPSG:{target_srid}'),
        ST_Transform(s.{osm_geom_col}, 'EPSG:4326', 'EPSG:{target_srid}')
    )
    """

    try:
        result_df = con.execute(sql).df()

        if len(result_df) > 0:
            # Convert WKB to geometry
            result_df['geometry'] = result_df['geom_wkb'].apply(lambda x: wkb.loads(bytes(x)))
            result_df = result_df.drop('geom_wkb', axis=1)

            newly_matched = gpd.GeoDataFrame(result_df, geometry='geometry', crs=target_crs)

            # Remove duplicates
            newly_matched = newly_matched.drop_duplicates(subset=['geometry'])

            print(f"  Successfully matched {len(newly_matched):,} buildings using OSM coordinates!")

            # Identify schools that are still unmatched
            matched_indices = set(newly_matched.index)
            still_unmatched = geocoded_schools[~geocoded_schools.index.isin(matched_indices)].copy()

            return newly_matched, still_unmatched
        else:
            print("  No additional matches found with OSM coordinates")
            return gpd.GeoDataFrame(), geocoded_schools

    except Exception as e:
        print(f"  ERROR during OSM retry matching: {e}")
        import traceback
        traceback.print_exc()
        return gpd.GeoDataFrame(), geocoded_schools
    finally:
        # Clean up temp file
        if temp_gpkg.exists():
            temp_gpkg.unlink()


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_country(country_name, config, use_osm=True):
    """Identify school footprints for a country using DuckDB, with optional OSM geocoding."""

    print("=" * 60)
    print(f"{country_name.upper()}: School Building Footprint Identification")
    print(f"Using {config['crs']} with DuckDB")
    if use_osm:
        print("OSM Geocoding: ENABLED")
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

    # ========================================================================
    # PART 5: Retry with 3-meter buffer for unmatched schools
    # ========================================================================
    if unmatched_schools is not None and len(unmatched_schools) > 0:
        print("\n[PART 5] Retrying unmatched schools with 3-meter buffer...")

        try:
            # Create buffered version of unmatched schools in DuckDB
            buffer_sql = f"""
            SELECT 
                s.* EXCLUDE {school_geom_col},
                ST_Buffer(
                    ST_Transform(s.{school_geom_col}, 'EPSG:{school_srid}', 'EPSG:{target_srid}'),
                    3.0
                ) as geom_buffered
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

            con.execute("CREATE OR REPLACE TABLE schools_buffered AS " + buffer_sql)

            # Try matching with buffered schools
            buffered_match_sql = f"""
            SELECT 
                b.* EXCLUDE {building_geom_col},
                s.rowid as school_rowid,
                ST_AsWKB(ST_Transform(b.{building_geom_col}, 'EPSG:{building_srid}', '{target_crs}')) AS geom_wkb
            FROM buildings b
            INNER JOIN schools_buffered s
            ON ST_Intersects(
                ST_Transform(b.{building_geom_col}, 'EPSG:{building_srid}', 'EPSG:{target_srid}'),
                s.geom_buffered
            )
            """

            buffered_result_df = con.execute(buffered_match_sql).df()

            if len(buffered_result_df) > 0:
                # Convert WKB to geometry
                buffered_result_df['geometry'] = buffered_result_df['geom_wkb'].apply(lambda x: wkb.loads(bytes(x)))
                buffered_result_df = buffered_result_df.drop('geom_wkb', axis=1)

                buffered_matches = gpd.GeoDataFrame(buffered_result_df, geometry='geometry', crs=target_crs)

                # Remove duplicates
                buffered_matches = buffered_matches.drop_duplicates(subset=['geometry'])

                print(f"  ✓ Found {len(buffered_matches):,} additional buildings with 3m buffer")

                # Combine with original school footprints
                school_footprints = pd.concat([school_footprints, buffered_matches], ignore_index=True)
                school_footprints = school_footprints.drop_duplicates(subset=['geometry'])

                # Update matched count
                newly_matched_school_ids = set(buffered_result_df['school_rowid'].unique())
                matched_count += len(newly_matched_school_ids)
                unmatched_count = school_count - matched_count

                # Update unmatched schools list
                unmatched_sql_updated = f"""
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

                    UNION

                    SELECT DISTINCT s3.rowid
                    FROM schools_buffered s3
                    INNER JOIN buildings b2
                    ON ST_Intersects(
                        ST_Transform(b2.{building_geom_col}, 'EPSG:{building_srid}', 'EPSG:{target_srid}'),
                        s3.geom_buffered
                    )
                )
                """

                unmatched_df_updated = con.execute(unmatched_sql_updated).df()

                if len(unmatched_df_updated) > 0:
                    unmatched_df_updated['geometry'] = unmatched_df_updated['geom_wkb'].apply(
                        lambda x: wkb.loads(bytes(x)))
                    unmatched_df_updated = unmatched_df_updated.drop('geom_wkb', axis=1)
                    unmatched_schools = gpd.GeoDataFrame(unmatched_df_updated, geometry='geometry', crs=target_crs)
                else:
                    unmatched_schools = None

                print(f"  Updated stats:")
                print(
                    f"    Schools WITH footprints: {matched_count:,}/{school_count:,} ({matched_count / school_count * 100:.2f}%)")
                print(
                    f"    Schools WITHOUT footprints: {unmatched_count:,}/{school_count:,} ({unmatched_count / school_count * 100:.2f}%)")
            else:
                print("  No additional matches found with 3m buffer")

            # Clean up
            con.execute("DROP TABLE IF EXISTS schools_buffered")

        except Exception as e:
            print(f"  Warning: Buffer retry failed: {e}")
            import traceback
            traceback.print_exc()

    # ========================================================================
    # PART 7: Save outputs
    # ========================================================================
    print("\n[PART 6] Saving outputs...")

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
    print(f"Unmatched schools: {len(unmatched_schools) if unmatched_schools is not None else 0:,}")
    print(f"CRS: {target_crs}")
    print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")
    print(f"{'=' * 60}\n")

    return True


# ============================================================================
# RUN
# ============================================================================

if __name__ == '__main__':
    # Process Canada with OSM enhancement
    process_country('canada', COUNTRIES['canada'], use_osm=True)

    # Process United States with OSM enhancement
    process_country('united_states', COUNTRIES['united_states'], use_osm=True)