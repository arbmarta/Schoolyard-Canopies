"""
Temporality: 2019 - 2020
Source: https://nces.ed.gov/programs/edge/geographic/schoollocations

CRS: EPSG:5070 (NAD83 / Conus Albers) - Equal-area projection in meters for US analysis
"""

import geopandas as gpd
from pathlib import Path
import pandas as pd

print("=" * 60)
print("Census Block & School Intersection Analysis")
print("Using EPSG:5070 (NAD83 Conus Albers) - meters")
print("=" * 60)

# Target CRS for all data
TARGET_EPSG = 5070
TARGET_CRS = 'EPSG:5070'

# ============================================================================
# PART 1: Load and merge school point data
# ============================================================================
print("\n[PART 1] Loading school data...")

schools_base = Path('schools/united_states')
private_dir = schools_base / 'private'
public_dir = schools_base / 'public'

# Find all school shapefiles
print("\n[Step 1.1] Finding school shapefiles...")
private_shps = list(private_dir.rglob('*.shp'))
public_shps = list(public_dir.rglob('*.shp'))

print(f"Found {len(private_shps)} private school shapefiles")
print(f"Found {len(public_shps)} public school shapefiles")

all_school_shps = private_shps + public_shps

if len(all_school_shps) == 0:
    print("ERROR: No school shapefiles found!")
    exit(1)

# Load and merge all school data
print("\n[Step 1.2] Loading and merging school data...")
school_gdfs = []

for i, shp_file in enumerate(all_school_shps, 1):
    try:
        print(f"Reading school file ({i}/{len(all_school_shps)}): {shp_file.name}")
        gdf = gpd.read_file(shp_file)

        print(f"  Original CRS: {gdf.crs}")

        # Verify it's point geometry
        geom_types = gdf.geometry.geom_type.unique()
        if not all(gt == 'Point' for gt in geom_types):
            print(f"  WARNING: Non-point geometries found: {geom_types}")
            # Filter to only points
            gdf = gdf[gdf.geometry.geom_type == 'Point']
            print(f"  Filtered to {len(gdf)} point features")

        # Reproject to target CRS
        print(f"  Reprojecting to {TARGET_CRS}")
        gdf = gdf.to_crs(TARGET_CRS)

        print(f"  Loaded {len(gdf)} schools")
        school_gdfs.append(gdf)

    except Exception as e:
        print(f"  ERROR reading {shp_file.name}: {e}")
        continue

if len(school_gdfs) == 0:
    print("ERROR: No school shapefiles could be read!")
    exit(1)

# After loading all GeoDataFrames, before concatenation
print(f"\n[Step 1.3] Validating columns and data types before concatenation...")

# Check column consistency
all_columns = set()
column_counts = {}

for i, gdf in enumerate(school_gdfs):
    cols = set(gdf.columns)
    all_columns.update(cols)

    # Count how many files have each column
    for col in cols:
        column_counts[col] = column_counts.get(col, 0) + 1

    print(f"  File {i + 1}: {len(gdf.columns)} columns, {len(gdf)} rows")

# Report columns that aren't in all files
print(f"\nTotal unique columns across all files: {len(all_columns)}")
missing_cols = {col: len(school_gdfs) - count
                for col, count in column_counts.items()
                if count < len(school_gdfs)}

if missing_cols:
    print(f"\nWARNING: {len(missing_cols)} columns missing from some files:")
    for col, missing_count in sorted(missing_cols.items(), key=lambda x: x[1], reverse=True):
        print(f"  '{col}': missing from {missing_count}/{len(school_gdfs)} files")

# Check data types for common columns
print("\n[Step 1.4] Checking data types for common columns...")
common_cols = [col for col, count in column_counts.items()
               if count == len(school_gdfs)]

for col in common_cols:
    dtypes = set(str(gdf[col].dtype) for gdf in school_gdfs if col in gdf.columns)
    if len(dtypes) > 1:
        print(f"  WARNING: Column '{col}' has inconsistent types: {dtypes}")

print(f"\n[Step 1.5] Concatenating {len(school_gdfs)} GeoDataFrames...")
schools_gdf = pd.concat(school_gdfs, ignore_index=True)

print(f"Total schools: {len(schools_gdf)}")
print(f"School CRS: {schools_gdf.crs}")
print(f"School EPSG: {TARGET_EPSG}")
print(f"Units: meters")

# Save concatenated schools to GeoPackage
print("\n[Step 1.6] Saving concatenated schools to GeoPackage...")
schools_output = schools_base / 'US_schools.gpkg'
schools_gdf.to_file(schools_output, driver='GPKG', layer='schools')
print(f"Saved {len(schools_gdf)} schools to: {schools_output}")
print(f"File size: {schools_output.stat().st_size / (1024 ** 2):.2f} MB")

# ============================================================================
# PART 2: Load census block data
# ============================================================================
print("\n[PART 2] Loading census block data...")

census_base = Path('census_blocks/united_states/state_level')

print("\n[Step 2.1] Finding census block shapefiles...")
census_shps = list(census_base.rglob('*.shp'))
print(f"Found {len(census_shps)} census block shapefiles")

if len(census_shps) == 0:
    print("ERROR: No census block shapefiles found!")
    exit(1)

# ============================================================================
# PART 3: Spatial intersection - filter census blocks with schools
# ============================================================================
print("\n[PART 3] Filtering census blocks by school intersection...")
print(f"All data will be processed in {TARGET_CRS}")

filtered_blocks = []

for i, shp_file in enumerate(census_shps, 1):
    try:
        print(f"\nProcessing census blocks ({i}/{len(census_shps)}): {shp_file.name}")
        census_gdf = gpd.read_file(shp_file)

        print(f"  Original CRS: {census_gdf.crs}")

        # Reproject census blocks to target CRS
        print(f"  Reprojecting to {TARGET_CRS}")
        census_gdf = census_gdf.to_crs(TARGET_CRS)

        print(f"  Total blocks in file: {len(census_gdf)}")

        # Spatial join to find blocks that intersect with schools
        intersecting = gpd.sjoin(
            census_gdf,
            schools_gdf,
            how='inner',
            predicate='intersects'
        )

        # Get unique census blocks (remove duplicates from multiple school intersections)
        unique_block_indices = intersecting.index.unique()
        blocks_with_schools = census_gdf.loc[unique_block_indices].copy()

        print(f"  Blocks with schools: {len(blocks_with_schools)}")

        if len(blocks_with_schools) > 0:
            filtered_blocks.append(blocks_with_schools)

    except Exception as e:
        print(f"  ERROR processing {shp_file.name}: {e}")
        continue

if len(filtered_blocks) == 0:
    print("\nERROR: No census blocks with schools found!")
    exit(1)

print(f"\n[Step 3.1] Concatenating filtered census blocks...")
master_gdf = pd.concat(filtered_blocks, ignore_index=True)

print(f"Total census blocks with schools: {len(master_gdf)}")
print(f"Total columns: {len(master_gdf.columns)}")
print(f"CRS: {master_gdf.crs}")
print(f"EPSG: {TARGET_EPSG}")

# ============================================================================
# PART 4: Remove duplicates and save
# ============================================================================
print("\n[PART 4] Finalizing and saving...")

print("\n[Step 4.1] Checking for duplicate geometries...")
initial_count = len(master_gdf)
master_gdf = master_gdf.drop_duplicates(subset=['geometry'])
final_count = len(master_gdf)
duplicates_removed = initial_count - final_count

if duplicates_removed > 0:
    print(f"Removed {duplicates_removed} duplicate geometries")
else:
    print("No duplicate geometries found")

# Save to GeoPackage
print("\n[Step 4.2] Saving master geodatabase...")
output_path = census_base.parent / 'US_census_blocks_with_schools.gpkg'

master_gdf.to_file(output_path, driver='GPKG', layer='census_blocks')

print(f"\n{'=' * 60}")
print(f"SUCCESS!")
print(f"{'=' * 60}")
print(f"Output file: {output_path}")
print(f"Total schools processed: {len(schools_gdf)}")
print(f"Total census blocks with schools: {final_count}")
print(f"CRS: {master_gdf.crs}")
print(f"EPSG: {TARGET_EPSG}")
print(f"Units: meters (equal-area projection)")
print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")
print(f"{'=' * 60}")