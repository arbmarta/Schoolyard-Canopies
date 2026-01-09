"""

Convert school records to point geometries and save as GeoPackage.

Sources:
- Canada (2019-2021): https://www.statcan.gc.ca/en/lode/databases/odef
- United States (2019-2020): https://nces.ed.gov/programs/edge/geographic/schoollocations
- United States (2019-2020): https://nces.ed.gov/ccd/files.asp#Fiscal:2,LevelId:7,SchoolYearId:34,Page:1
- United States (2019-2020): https://nces.ed.gov/surveys/pss/pssdata.asp

Excludes schools in the Arctic Cordillera and Tundra
- Based on terrestrial ecoregions Level 1 (cec.org/north-american-environmental-atlas/terrestrial-ecoregions-level-i/)

CRS:
- Canada: EPSG:3347 (Statistics Canada Lambert)
- United States: EPSG:5070 (NAD83 / Conus Albers)
Both are equal-area projections in meters

ISCED Classification:
- ISCED010: Pre-kindergarten (early childhood education)
- ISCED020: Kindergarten
- ISCED1: Elementary (grades 1-6)
- ISCED2: Junior secondary (grades 7-9)
- ISCED3: Senior secondary (grades 10-12)
- ISCED4Plus: Post-secondary (excluded from analysis)

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
        'grades_public_csv': '../inputs/schools/united_states/public/school_grades_public.csv',
        'grades_private_csv': '../inputs/schools/united_states/private/school_grades_private.csv',
        'output_path': '../inputs/schools/united_states/us_school_points.gpkg'
    }
}

north_of_tree_line = gpd.read_file('../inputs/ecoregions_level1/NA_Terrestrial_Ecoregions_v2_level1.shp')
north_of_tree_line = north_of_tree_line[north_of_tree_line['NameL1_En'].isin(['Arctic Cordillera', 'Tundra'])]


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

    # Filter out schools north of tree line
    print("\n[PART 3] Filtering schools north of tree line...")
    tree_line = north_of_tree_line.to_crs(config['crs'])

    # Find schools within Arctic Cordillera/Tundra regions
    schools_in_arctic = gpd.sjoin(schools_gdf, tree_line, how='inner', predicate='within')
    north_count = len(schools_in_arctic)

    print(f"Schools north of tree line (Arctic Cordillera/Tundra): {north_count}")

    # Keep only schools NOT in these regions
    schools_gdf = schools_gdf[~schools_gdf.index.isin(schools_in_arctic.index)]
    print(f"Records after filtering: {len(schools_gdf)}")

    return schools_gdf, config


# ============================================================================
# HELPER: Create ISCED columns from grade data
# ============================================================================

def create_isced_columns(df):
    """
    Create ISCED level indicator columns (0/1) based on grades offered.

    ISCED Classification:
    - ISCED010: Pre-kindergarten (PK)
    - ISCED020: Kindergarten (KG)
    - ISCED1: Elementary (grades 1-6)
    - ISCED2: Junior secondary (grades 7-9)
    - ISCED3: Senior secondary (grades 10-12)
    """

    # Initialize all ISCED columns to 0
    df['ISCED010'] = 0
    df['ISCED020'] = 0
    df['ISCED1'] = 0
    df['ISCED2'] = 0
    df['ISCED3'] = 0

    # For public schools: use G_*_OFFERED columns
    if 'G_PK_OFFERED' in df.columns:
        df.loc[df['G_PK_OFFERED'] == 'Yes', 'ISCED010'] = 1

    if 'G_KG_OFFERED' in df.columns:
        df.loc[df['G_KG_OFFERED'] == 'Yes', 'ISCED020'] = 1

    # ISCED1: Grades 1-6
    for grade in ['G_1_OFFERED', 'G_2_OFFERED', 'G_3_OFFERED',
                  'G_4_OFFERED', 'G_5_OFFERED', 'G_6_OFFERED']:
        if grade in df.columns:
            df.loc[df[grade] == 'Yes', 'ISCED1'] = 1

    # ISCED2: Grades 7-9
    for grade in ['G_7_OFFERED', 'G_8_OFFERED', 'G_9_OFFERED']:
        if grade in df.columns:
            df.loc[df[grade] == 'Yes', 'ISCED2'] = 1

    # ISCED3: Grades 10-12
    for grade in ['G_10_OFFERED', 'G_11_OFFERED', 'G_12_OFFERED']:
        if grade in df.columns:
            df.loc[df[grade] == 'Yes', 'ISCED3'] = 1

    # For private schools: use LOGR2020 and HIGR2020
    if 'LOGR2020' in df.columns and 'HIGR2020' in df.columns:
        for idx, row in df.iterrows():
            if pd.isna(row['LOGR2020']) or pd.isna(row['HIGR2020']):
                continue

            low = str(row['LOGR2020']).upper()
            high = str(row['HIGR2020']).upper()

            # Pre-K
            if low == 'PK' or high == 'PK':
                df.loc[idx, 'ISCED010'] = 1

            # Kindergarten
            if low == 'KG' or (low <= 'KG' <= high):
                df.loc[idx, 'ISCED020'] = 1

            # Try to convert to numeric for grade ranges
            try:
                # Convert grade codes to numbers
                def grade_to_num(g):
                    g = str(g).upper()
                    if g == 'PK':
                        return -2
                    elif g == 'KG':
                        return 0
                    else:
                        return int(g)

                low_num = grade_to_num(low)
                high_num = grade_to_num(high)

                # ISCED1: Grades 1-6
                if (low_num <= 6 and high_num >= 1):
                    df.loc[idx, 'ISCED1'] = 1

                # ISCED2: Grades 7-9
                if (low_num <= 9 and high_num >= 7):
                    df.loc[idx, 'ISCED2'] = 1

                # ISCED3: Grades 10-12
                if (low_num <= 12 and high_num >= 10):
                    df.loc[idx, 'ISCED3'] = 1

            except (ValueError, TypeError):
                # If conversion fails, skip
                pass

    return df


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

    # Load grade data for both public and private schools
    print("\n[PART 4] Loading and merging grade data...")

    # Load public school grades
    print("  Loading public school grades...")
    grades_public = pd.read_csv(config['grades_public_csv'], encoding='latin1', low_memory=False)
    print(f"  Loaded {len(grades_public)} public grade records")

    # Load private school grades
    print("  Loading private school grades...")
    grades_private = pd.read_csv(config['grades_private_csv'], encoding='latin1', low_memory=False)
    print(f"  Loaded {len(grades_private)} private grade records")

    # Merge public schools on NCESSCH
    if 'NCESSCH' in schools_gdf.columns:
        schools_gdf['NCESSCH'] = schools_gdf['NCESSCH'].astype(str)
        grades_public['NCESSCH'] = grades_public['NCESSCH'].astype(str)

        print("  Merging public school grades on NCESSCH...")
        schools_gdf = schools_gdf.merge(
            grades_public[['NCESSCH', 'GSLO', 'GSHI', 'LEVEL', 'G_PK_OFFERED', 'G_KG_OFFERED',
                           'G_1_OFFERED', 'G_2_OFFERED', 'G_3_OFFERED', 'G_4_OFFERED',
                           'G_5_OFFERED', 'G_6_OFFERED', 'G_7_OFFERED', 'G_8_OFFERED',
                           'G_9_OFFERED', 'G_10_OFFERED', 'G_11_OFFERED', 'G_12_OFFERED']],
            on='NCESSCH',
            how='left',
            suffixes=('', '_public')
        )

    # Merge private schools on PPIN
    if 'PPIN' in schools_gdf.columns:
        schools_gdf['PPIN'] = schools_gdf['PPIN'].astype(str)
        grades_private['PPIN'] = grades_private['PPIN'].astype(str)

        print("  Merging private school grades on PPIN...")
        schools_gdf = schools_gdf.merge(
            grades_private[['PPIN', 'LOGR2020', 'HIGR2020', 'LEVEL', 'LEVEL2']],
            on='PPIN',
            how='left',
            suffixes=('', '_private')
        )

    # Create ISCED columns
    print("\n[PART 5] Creating ISCED classification columns...")
    schools_gdf = create_isced_columns(schools_gdf)

    # Print ISCED statistics
    print(f"\n  ISCED level distribution:")
    print(f"  ISCED010 (Pre-K): {schools_gdf['ISCED010'].sum()} schools")
    print(f"  ISCED020 (Kindergarten): {schools_gdf['ISCED020'].sum()} schools")
    print(f"  ISCED1 (Elementary, grades 1-6): {schools_gdf['ISCED1'].sum()} schools")
    print(f"  ISCED2 (Junior secondary, grades 7-9): {schools_gdf['ISCED2'].sum()} schools")
    print(f"  ISCED3 (Senior secondary, grades 10-12): {schools_gdf['ISCED3'].sum()} schools")

    # Count schools with multiple ISCED levels
    schools_gdf['ISCED_count'] = (schools_gdf['ISCED010'] + schools_gdf['ISCED020'] +
                                  schools_gdf['ISCED1'] + schools_gdf['ISCED2'] +
                                  schools_gdf['ISCED3'])

    multi_level = (schools_gdf['ISCED_count'] > 1).sum()
    print(f"\n  Schools with multiple ISCED levels: {multi_level} ({multi_level / len(schools_gdf) * 100:.1f}%)")

    # Print grade data merge statistics
    print(f"\n  Grade data merge statistics:")
    public_total = (schools_gdf['SCHOOL_TYPE'] == 'Public').sum()
    public_with_grades = ((schools_gdf['SCHOOL_TYPE'] == 'Public') & schools_gdf['GSLO'].notna()).sum()
    print(
        f"  Public schools with grades: {public_with_grades}/{public_total} ({public_with_grades / public_total * 100:.1f}%)")

    private_total = (schools_gdf['SCHOOL_TYPE'] == 'Private').sum()
    private_with_grades = ((schools_gdf['SCHOOL_TYPE'] == 'Private') & schools_gdf['LOGR2020'].notna()).sum()
    print(
        f"  Private schools with grades: {private_with_grades}/{private_total} ({private_with_grades / private_total * 100:.1f}%)")

    # Filter out schools north of tree line
    print("\n[PART 6] Filtering schools north of tree line...")
    tree_line = north_of_tree_line.to_crs(config['crs'])

    # Find schools within Arctic Cordillera/Tundra regions (should be minimal for US)
    schools_in_arctic = gpd.sjoin(schools_gdf, tree_line, how='inner', predicate='within')
    north_count = len(schools_in_arctic)

    print(f"Schools north of tree line (Arctic Cordillera/Tundra): {north_count}")

    # Keep only schools NOT in these regions
    schools_gdf = schools_gdf[~schools_gdf.index.isin(schools_in_arctic.index)]
    print(f"Records after filtering: {len(schools_gdf)}")

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

    # Always use 'schools' layer name for consistency
    schools_gdf.to_file(output_path, driver='GPKG', layer='schools')

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