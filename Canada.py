"""
Temporality: 2019 - 2021
Source: https://www.statcan.gc.ca/en/lode/databases/odef

CRS: EPSG:3347 (Statistics Canada Lambert) - Equal-area projection in meters for Canada
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path
import numpy as np

# Target CRS for Canada
TARGET_EPSG = 3347
TARGET_CRS = 'EPSG:3347'

print("=" * 60)
print("Canadian Census Blocks & Schools Intersection Analysis")
print(f"Using {TARGET_CRS} (Statistics Canada Lambert) - meters")
print("=" * 60)

# ============================================================================
# PART 1: Load and filter school data
# ============================================================================
print("\n[PART 1] Loading and filtering school data...")

can_df = pd.read_csv('schools/canada/odef_v2/ODEF_v2_1.csv', encoding='latin1')

# Convert ISCED columns to numeric (they're read as strings)
isced_cols = ['ISCED010', 'ISCED020', 'ISCED1', 'ISCED2', 'ISCED3', 'ISCED4Plus']
for col in isced_cols:
    can_df[col] = pd.to_numeric(can_df[col], errors='coerce')

print(f"Total dataset length: {len(can_df)}")
print(f"Post-secondary records (ISCED4Plus == 1): {(can_df['ISCED4Plus'] == 1).sum()}")

# Only drop rows where ISCED4Plus is 1 AND all other ISCED levels are 0
exclusively_post_secondary = (
    (can_df['ISCED4Plus'] == 1) &
    (can_df['ISCED010'] == 0) &
    (can_df['ISCED020'] == 0) &
    (can_df['ISCED1'] == 0) &
    (can_df['ISCED2'] == 0) &
    (can_df['ISCED3'] == 0)
)

print(f"Exclusively post-secondary records: {exclusively_post_secondary.sum()}")

# Drop exclusively post-secondary facilities
can_df = can_df[~exclusively_post_secondary]

print(f"Records after filtering: {len(can_df)}")
print(f"Facility types remaining: {len(can_df['Facility_Type'].unique())} unique types")

# ============================================================================
# PART 2: Create point geometries from Latitude/Longitude
# ============================================================================
print("\n[PART 2] Creating point geometries from coordinates...")

# Convert coordinate columns to numeric (handles '..' and other invalid values)
print(f"Converting coordinate columns to numeric...")
can_df['Latitude'] = pd.to_numeric(can_df['Latitude'], errors='coerce')
can_df['Longitude'] = pd.to_numeric(can_df['Longitude'], errors='coerce')

# Check for invalid coordinates
invalid_coords = can_df['Latitude'].isna() | can_df['Longitude'].isna()
print(f"Records with invalid coordinates: {invalid_coords.sum()}")

# Remove rows with missing or invalid coordinates
print(f"Records before removing invalid coordinates: {len(can_df)}")
can_df = can_df.dropna(subset=['Latitude', 'Longitude'])
print(f"Records after removing invalid coordinates: {len(can_df)}")

if len(can_df) == 0:
    print("ERROR: No valid coordinates found!")
    exit(1)

# Create geometry column from Latitude/Longitude (WGS84)
geometry = [Point(xy) for xy in zip(can_df['Longitude'], can_df['Latitude'])]
schools_gdf = gpd.GeoDataFrame(can_df, geometry=geometry, crs='EPSG:4326')

print(f"Created {len(schools_gdf)} school points")
print(f"Original CRS: {schools_gdf.crs} (WGS84)")

# Reproject to Statistics Canada Lambert
print(f"Reprojecting to {TARGET_CRS}...")
schools_gdf = schools_gdf.to_crs(TARGET_CRS)
print(f"Reprojected CRS: {schools_gdf.crs}")

# ============================================================================
# PART 3: Load census blocks and filter by school intersection
# ============================================================================
print("\n[PART 3] Loading census blocks and filtering by school intersection...")

census_path = Path('census_blocks/canada/census_boundary_files/ldb_000b21a_e.shp')

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
# PART 4: Spatial join to filter census blocks with schools
# ============================================================================
print("\n[PART 4] Performing spatial intersection...")

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
# PART 5: Save filtered census blocks
# ============================================================================
print("\n[PART 5] Saving filtered census blocks...")

output_path = Path('census_blocks/canada/Canada_census_blocks_with_schools.gpkg')

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