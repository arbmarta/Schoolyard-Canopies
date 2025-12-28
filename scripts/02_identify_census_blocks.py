"""
Identify census blocks that contain schools through spatial intersection.

Temporality: 2019 - 2021
CRS: EPSG:3347 (Statistics Canada Lambert) - Equal-area projection in meters for Canada
"""

import geopandas as gpd
from pathlib import Path

# Target CRS for Canada
TARGET_EPSG = 3347
TARGET_CRS = 'EPSG:3347'

print("=" * 60)
print("Census Blocks & Schools Intersection Analysis")
print(f"Using {TARGET_CRS} (Statistics Canada Lambert) - meters")
print("=" * 60)

# ============================================================================
# PART 1: Load school points
# ============================================================================
print("\n[PART 1] Loading school points...")

schools_path = Path('inputs/schools/canada/canada_school_points.gpkg')

if not schools_path.exists():
    print(f"ERROR: School points file not found at {schools_path}")
    print("Please run 01_school_records_to_points.py first")
    exit(1)

schools_gdf = gpd.read_file(schools_path)
print(f"Loaded {len(schools_gdf)} school points")
print(f"CRS: {schools_gdf.crs}")

# ============================================================================
# PART 2: Load census blocks
# ============================================================================
print("\n[PART 2] Loading census blocks...")

census_path = Path('inputs/census_blocks/canada/census_boundary_files/ldb_000b21a_e.shp')

if not census_path.exists():
    print(f"ERROR: Census block shapefile not found at {census_path}")
    exit(1)

print(f"Reading census blocks from: {census_path.name}")
census_gdf = gpd.read_file(census_path)

print(f"Total census blocks: {len(census_gdf)}")
print(f"Original census CRS: {census_gdf.crs}")

# Reproject census blocks to match schools
if census_gdf.crs.to_epsg() != TARGET_EPSG:
    print(f"Reprojecting census blocks to {TARGET_CRS}...")
    census_gdf = census_gdf.to_crs(TARGET_CRS)
    print(f"Census blocks reprojected")

# ============================================================================
# PART 3: Spatial join to filter census blocks with schools
# ============================================================================
print("\n[PART 3] Performing spatial intersection...")

# Spatial join to find blocks that contain schools
intersecting = gpd.sjoin(
    census_gdf,
    schools_gdf,
    how='inner',
    predicate='intersects'
)

print(f"Total intersections found: {len(intersecting)}")

# Get unique census blocks (remove duplicates from multiple school intersections)
unique_block_indices = intersecting.index.unique()
blocks_with_schools = census_gdf.loc[unique_block_indices].copy()

print(f"Census blocks containing schools: {len(blocks_with_schools)}")

if len(blocks_with_schools) == 0:
    print("WARNING: No census blocks with schools found!")
    exit(1)

# ============================================================================
# PART 4: Save filtered census blocks
# ============================================================================
print("\n[PART 4] Saving filtered census blocks...")

output_path = Path('inputs/census_blocks/canada/Canada_census_blocks_with_schools.gpkg')
output_path.parent.mkdir(parents=True, exist_ok=True)

blocks_with_schools.to_file(output_path, driver='GPKG', layer='census_blocks')

print(f"\n{'=' * 60}")
print(f"SUCCESS!")
print(f"{'=' * 60}")
print(f"Output file: {output_path}")
print(f"Total schools processed: {len(schools_gdf)}")
print(f"Total census blocks with schools: {len(blocks_with_schools)}")
print(f"CRS: {blocks_with_schools.crs}")
print(f"EPSG: {TARGET_EPSG}")
print(f"Units: meters (equal-area projection)")
print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")
print(f"{'=' * 60}")

"""
Identify census blocks that contain schools through spatial intersection.

Temporality: 2019 - 2020
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
# PART 1: Load school points
# ============================================================================
print("\n[PART 1] Loading school points...")

schools_path = Path('inputs/schools/united_states/US_school_points.gpkg')

if not schools_path.exists():
    print(f"ERROR: School points file not found at {schools_path}")
    print("Please run 01_school_records_to_points.py first")
    exit(1)

schools_gdf = gpd.read_file(schools_path, layer='schools')
print(f"Loaded {len(schools_gdf)} school points")
print(f"CRS: {schools_gdf.crs}")

# ============================================================================
# PART 2: Check if processed census blocks already exist
# ============================================================================
census_base = Path('inputs/census_blocks/united_states/state_level')
output_path = census_base.parent / 'US_census_blocks_with_schools.gpkg'

if output_path.exists():
    print(f"\n[PART 2] Found existing processed census blocks file!")
    print(f"Loading from: {output_path}")
    master_gdf = gpd.read_file(output_path, layer='census_blocks')
    print(f"Loaded {len(master_gdf)} census blocks with schools")
    print(f"CRS: {master_gdf.crs}")
    print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")

    # Skip to final summary
    final_count = len(master_gdf)

else:
    # ============================================================================
    # PART 2: Load census block data
    # ============================================================================
    print("\n[PART 2] No processed file found - will process census blocks...")
    print("Loading census block data...")

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

    print("\n[Step 4.1] Checking for duplicate census blocks...")
    initial_count = len(master_gdf)

    # Remove duplicates based on GEOID20 (unique census block identifier)
    master_gdf = master_gdf.drop_duplicates(subset=['GEOID20'], keep='first')

    final_count = len(master_gdf)
    duplicates_removed = initial_count - final_count

    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate census blocks (based on GEOID20)")
    else:
        print("No duplicate census blocks found")

    print(f"Unique census blocks remaining: {final_count}")

    # Save to GeoPackage
    print("\n[Step 4.2] Saving master geodatabase...")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    master_gdf.to_file(output_path, driver='GPKG', layer='census_blocks')

    print(f"Saved to: {output_path}")
    print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
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