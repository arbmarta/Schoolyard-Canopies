"""
Convert school records to point geometries and save as GeoPackage.

Sources:
- Canada (2019-2021): https://www.statcan.gc.ca/en/lode/databases/odef
- United States (2019-2020): https://nces.ed.gov/programs/edge/geographic/schoollocations

CRS:
- Canada: EPSG:3347 (Statistics Canada Lambert)
- United States: EPSG:5070 (NAD83 / Conus Albers)
Both are equal-area projections in meters
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

COUNTRIES = {
    'canada': {
        'epsg': 3347,
        'crs': 'EPSG:3347',
        'name': 'Statistics Canada Lambert',
        'csv_path': '../inputs/schools/canada/odef_v2/ODEF_v2_1.csv',
        'output_path': '../inputs/schools/canada/canada_school_points.gpkg'
    },
    'united_states': {
        'epsg': 5070,
        'crs': 'EPSG:5070',
        'name': 'NAD83 Conus Albers',
        'schools_base': '../inputs/schools/united_states',
        'grades_csv': '../inputs/schools/united_states/school_grades.csv',
        'output_path': '../inputs/schools/united_states/US_school_points.gpkg'
    }
}


# ============================================================================
# CANADA: Process CSV data
# ============================================================================

def process_canada():
    """Process Canadian school data from CSV."""
    config = COUNTRIES['canada']

    print("=" * 60)
    print("Canadian School Records to Point Geometries")
    print(f"Using {config['crs']} ({config['name']}) - meters")
    print("=" * 60)

    # Load CSV
    print("\n[PART 1] Loading and filtering school data...")
    can_df = pd.read_csv(config['csv_path'], encoding='latin1')

    # Convert ISCED columns to numeric
    isced_cols = ['ISCED010', 'ISCED020', 'ISCED1', 'ISCED2', 'ISCED3', 'ISCED4Plus']
    for col in isced_cols:
        can_df[col] = pd.to_numeric(can_df[col], errors='coerce')

    print(f"Total dataset length: {len(can_df)}")
    print(f"Post-secondary records (ISCED4Plus == 1): {(can_df['ISCED4Plus'] == 1).sum()}")

    # Filter out exclusively post-secondary institutions
    exclusively_post_secondary = (
            (can_df['ISCED4Plus'] == 1) &
            (can_df['ISCED010'] == 0) &
            (can_df['ISCED020'] == 0) &
            (can_df['ISCED1'] == 0) &
            (can_df['ISCED2'] == 0) &
            (can_df['ISCED3'] == 0)
    )

    print(f"Exclusively post-secondary records: {exclusively_post_secondary.sum()}")
    can_df = can_df[~exclusively_post_secondary]
    print(f"Records after filtering: {len(can_df)}")

    # Create point geometries
    print("\n[PART 2] Creating point geometries from coordinates...")
    can_df['Latitude'] = pd.to_numeric(can_df['Latitude'], errors='coerce')
    can_df['Longitude'] = pd.to_numeric(can_df['Longitude'], errors='coerce')

    # Remove invalid coordinates
    invalid_coords = can_df['Latitude'].isna() | can_df['Longitude'].isna()
    print(f"Records with invalid coordinates: {invalid_coords.sum()}")
    can_df = can_df.dropna(subset=['Latitude', 'Longitude'])
    print(f"Records after removing invalid coordinates: {len(can_df)}")

    if len(can_df) == 0:
        print("ERROR: No valid coordinates found!")
        return None

    # Create GeoDataFrame
    geometry = [Point(xy) for xy in zip(can_df['Longitude'], can_df['Latitude'])]
    schools_gdf = gpd.GeoDataFrame(can_df, geometry=geometry, crs='EPSG:4326')

    print(f"Created {len(schools_gdf)} school points")
    print(f"Reprojecting to {config['crs']}...")
    schools_gdf = schools_gdf.to_crs(config['crs'])

    return schools_gdf, config


# ============================================================================
# UNITED STATES: Process shapefiles
# ============================================================================

def process_united_states():
    """Process US school data from multiple shapefiles."""
    config = COUNTRIES['united_states']

    print("=" * 60)
    print("US School Records to Point Geometries")
    print(f"Using {config['crs']} ({config['name']}) - meters")
    print("=" * 60)

    schools_base = Path(config['schools_base'])

    # Find all school shapefiles
    print("\n[PART 1] Finding school shapefiles...")
    private_shps = list((schools_base / 'private').rglob('*.shp'))
    public_shps = list((schools_base / 'public').rglob('*.shp'))
    all_school_shps = private_shps + public_shps

    print(f"Found {len(private_shps)} private school shapefiles")
    print(f"Found {len(public_shps)} public school shapefiles")

    if len(all_school_shps) == 0:
        print("ERROR: No school shapefiles found!")
        return None

    # Load and merge all shapefiles
    print("\n[PART 2] Loading and merging school data...")
    school_gdfs = []

    for i, shp_file in enumerate(all_school_shps, 1):
        try:
            print(f"Reading ({i}/{len(all_school_shps)}): {shp_file.name}")
            gdf = gpd.read_file(shp_file)

            # Add school type
            gdf['SCHOOL_TYPE'] = 'Private' if 'private' in str(shp_file) else 'Public'

            # Filter to points only
            if not all(gdf.geometry.geom_type == 'Point'):
                print(f"  WARNING: Filtering to point geometries only")
                gdf = gdf[gdf.geometry.geom_type == 'Point']

            # Reproject
            gdf = gdf.to_crs(config['crs'])
            print(f"  Loaded {len(gdf)} schools")
            school_gdfs.append(gdf)

        except Exception as e:
            print(f"  ERROR reading {shp_file.name}: {e}")
            continue

    if len(school_gdfs) == 0:
        print("ERROR: No school shapefiles could be read!")
        return None

    # Concatenate
    print("\n[PART 3] Concatenating shapefiles...")
    schools_gdf = pd.concat(school_gdfs, ignore_index=True)
    print(f"Total schools before merge: {len(schools_gdf)}")
    print(f"  Public schools: {(schools_gdf['SCHOOL_TYPE'] == 'Public').sum()}")
    print(f"  Private schools: {(schools_gdf['SCHOOL_TYPE'] == 'Private').sum()}")

    # Load and merge grade data (only for public schools with NCESSCH)
    print("\n[PART 4] Loading and merging grade data...")
    grades_df = pd.read_csv(config['grades_csv'])
    print(f"Loaded {len(grades_df)} grade records")

    # Check if NCESSCH column exists (it will for public schools)
    if 'NCESSCH' in schools_gdf.columns:
        # Convert to string for matching
        schools_gdf['NCESSCH'] = schools_gdf['NCESSCH'].astype(str)
        grades_df['NCESSCH'] = grades_df['NCESSCH'].astype(str)

        print(f"Merging on NCESSCH (public schools only)...")
        schools_gdf = schools_gdf.merge(
            grades_df,
            on='NCESSCH',
            how='left',
            suffixes=('', '_grades')
        )

        print(f"Total schools after merge: {len(schools_gdf)}")
        matched = schools_gdf['GSLO'].notna().sum()
        print(f"Schools with grade data: {matched} ({matched / len(schools_gdf) * 100:.1f}%)")

        public_with_grades = ((schools_gdf['SCHOOL_TYPE'] == 'Public') & schools_gdf['GSLO'].notna()).sum()
        public_total = (schools_gdf['SCHOOL_TYPE'] == 'Public').sum()
        print(
            f"  Public schools with grades: {public_with_grades}/{public_total} ({public_with_grades / public_total * 100:.1f}%)")
        print(f"  Private schools (no grade data available): {(schools_gdf['SCHOOL_TYPE'] == 'Private').sum()}")
    else:
        print("WARNING: No NCESSCH column found - skipping grade merge")

    return schools_gdf, config


# ============================================================================
# MAIN: Save outputs
# ============================================================================

def save_schools(schools_gdf, config):
    """Save school points to GeoPackage."""
    if schools_gdf is None:
        return

    output_path = Path(config['output_path'])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Determine layer name
    layer = 'schools' if 'united_states' in str(output_path) else None

    if layer:
        schools_gdf.to_file(output_path, driver='GPKG', layer=layer)
    else:
        schools_gdf.to_file(output_path, driver='GPKG')

    print(f"\n{'=' * 60}")
    print(f"SUCCESS!")
    print(f"{'=' * 60}")
    print(f"Output file: {output_path}")
    print(f"Total schools: {len(schools_gdf)}")
    print(f"CRS: {schools_gdf.crs}")
    print(f"EPSG: {config['epsg']}")
    print(f"Units: meters (equal-area projection)")
    print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")
    print(f"{'=' * 60}\n")


# ============================================================================
# RUN
# ============================================================================

if __name__ == '__main__':
    # Process Canada
    result = process_canada()
    if result:
        save_schools(*result)

    # Process United States
    result = process_united_states()
    if result:
        save_schools(*result)