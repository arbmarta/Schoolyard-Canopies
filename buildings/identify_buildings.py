"""
Complete workflow: Identify tiles from census blocks and create download pattern
"""

import geopandas as gpd
from pathlib import Path
import pandas as pd
from ftplib import FTP

print("=" * 60)
print("GlobalBuildingAtlas Download Preparation")
print("=" * 60)

# ============================================================================
# PART 1: Load census blocks that contain schools
# ============================================================================

print("\n[PART 1] Loading census blocks with schools...")

# Census block paths
us_census_path = Path('../census_blocks/united_states/US_census_blocks_with_schools.gpkg')
canada_census_path = Path('../census_blocks/canada/Canada_census_blocks_with_schools.gpkg')

census_blocks = []

# Load US census blocks
if us_census_path.exists():
    print(f"\nLoading US census blocks: {us_census_path}")
    us_blocks = gpd.read_file(us_census_path)
    us_blocks = us_blocks.to_crs('EPSG:4326')
    print(f"  Loaded {len(us_blocks)} census blocks")
    census_blocks.append(us_blocks)
else:
    print(f"\nWARNING: US census blocks not found at {us_census_path}")

# Load Canada census blocks
if canada_census_path.exists():
    print(f"\nLoading Canada census blocks: {canada_census_path}")
    canada_blocks = gpd.read_file(canada_census_path)
    canada_blocks = canada_blocks.to_crs('EPSG:4326')
    print(f"  Loaded {len(canada_blocks)} census blocks")
    census_blocks.append(canada_blocks)
else:
    print(f"\nWARNING: Canada census blocks not found at {canada_census_path}")

if not census_blocks:
    print("\nERROR: No census block files found!")
    exit(1)

# Combine all census blocks
print("\nCombining census blocks...")
all_blocks = gpd.GeoDataFrame(
    pd.concat(census_blocks, ignore_index=True),
    crs='EPSG:4326'
)
print(f"Total census blocks with schools: {len(all_blocks)}")


# ============================================================================
# PART 2: Identify required tiles from census block geometries
# ============================================================================

def get_tile_name(lat, lon):
    """Get tile name for a given lat/lon coordinate"""
    tile_lat = (lat // 5) * 5
    tile_lon = (lon // 5) * 5
    lat_str = f"N{int(tile_lat):02d}"
    lon_str = f"W{abs(int(tile_lon)):03d}"
    return f"{lat_str}_{lon_str}"


def get_tiles_for_geometry(geometry):
    """Get all tiles that a geometry overlaps"""
    tiles = set()
    bounds = geometry.bounds
    minx, miny, maxx, maxy = bounds

    lats = range(int(miny), int(maxy) + 2)
    lons = range(int(minx), int(maxx) + 2)

    for lat in lats:
        for lon in lons:
            tile = get_tile_name(lat, lon)
            tiles.add(tile)

    return tiles


print("\n[PART 2] Identifying required tiles from census blocks...")

required_tiles = set()

for idx, row in all_blocks.iterrows():
    if idx % 1000 == 0:
        print(f"  Processing block {idx}/{len(all_blocks)}...", end='\r')

    try:
        geometry = row.geometry
        block_tiles = get_tiles_for_geometry(geometry)
        required_tiles.update(block_tiles)
    except Exception as e:
        continue

print(f"\n  Completed processing {len(all_blocks)} census blocks")
print(f"\nTotal unique tiles needed: {len(required_tiles)}")

# Sort tiles
sorted_tiles = sorted(list(required_tiles))

# Save tile list
tile_list_file = '../outputs/required_building_tiles_from_census.txt'
with open(tile_list_file, 'w') as f:
    f.write("Required GlobalBuildingAtlas Tiles\n")
    f.write("=" * 60 + "\n\n")
    f.write(f"Total tiles: {len(sorted_tiles)}\n\n")
    for tile in sorted_tiles:
        f.write(f"{tile}\n")

print(f"Tile list saved to: {tile_list_file}")

# ============================================================================
# PART 3: Map tile names to server filenames
# ============================================================================

print("\n[PART 3] Mapping tile names to server filenames...")


def tile_to_filename(tile_name):
    """
    Example:
    N45_W075 -> covers 45-50°N, 70-75°W
    Server file: w075_n50_w070_n45.geojson
                 (west=75, north=50, east=70, south=45)
    """
    parts = tile_name.split('_')
    lat_str = parts[0][1:]
    lon_str = parts[1][1:]

    lat_south = int(lat_str)  # 45
    lat_north = lat_south + 5  # 50

    lon_west = int(lon_str)  # 75
    lon_east = lon_west - 5  # 70

    # Server format: w[west]_n[north]_w[east]_n[south]
    filename = f"w{lon_west:03d}_n{lat_north:02d}_w{lon_east:03d}_n{lat_south:02d}"

    return filename


# Create mapping
tile_mapping = {}
for tile in sorted_tiles:
    filename = tile_to_filename(tile)
    tile_mapping[tile] = filename

# Show sample mapping
print("\nSample tile mappings (first 10):")
print("-" * 60)
for tile in sorted_tiles[:10]:
    print(f"{tile:15s} -> {tile_mapping[tile]}.geojson")
print(f"... ({len(sorted_tiles) - 10} more tiles)")

# ============================================================================
# PART 4: Create rsync include pattern
# ============================================================================

print("\n[PART 4] Creating rsync download pattern...")

rsync_pattern_file = '../outputs/rsync_include_pattern_lod1_only.txt'

with open(rsync_pattern_file, 'w') as f:
    f.write("# rsync include pattern for GlobalBuildingAtlas\n")
    f.write("# LoD1 building models ONLY\n")
    f.write(f"# Total tiles: {len(sorted_tiles)}\n\n")

    f.write("# Include directory structure\n")
    f.write("+ LoD1/\n")
    f.write("+ LoD1/northamerica/\n")
    f.write("\n")

    f.write("# Include specific LoD1 tile files\n")
    for tile in sorted_tiles:
        filename = tile_mapping[tile]
        f.write(f"+ LoD1/northamerica/{filename}.geojson\n")

    f.write("\n# Exclude everything else\n")
    f.write("- *\n")

print(f"rsync pattern saved to: {rsync_pattern_file}")

# Save tile mapping reference
mapping_file = '../outputs/tile_to_filename_mapping.txt'
with open(mapping_file, 'w') as f:
    f.write("Tile Name -> Server Filename Mapping\n")
    f.write("=" * 60 + "\n\n")
    for tile in sorted_tiles:
        filename = tile_mapping[tile]
        f.write(f"{tile:15s} -> {filename}.geojson\n")

print(f"Tile mapping saved to: {mapping_file}")

# ============================================================================
# PART 5: Estimate download size
# ============================================================================

print("\n[PART 5] Estimating download size...")

major_urban_tiles = [
    'N45_W075',  # Ottawa/Montreal: ~7.6 GB
    'N40_W075',  # NYC/Philadelphia: ~6.5 GB
    'N40_W080',  # Pittsburgh/Cleveland: ~5.3 GB
    'N45_W080',  # Toronto: ~5.8 GB
    'N45_W085',  # Detroit/Chicago: ~5.7 GB
    'N40_W085',  # Indianapolis: ~5.0 GB
]

urban_in_list = [t for t in sorted_tiles if t in major_urban_tiles]
other_count = len(sorted_tiles) - len(urban_in_list)

print(f"\nMajor urban tiles (~1-8 GB each): {len(urban_in_list)}")
if urban_in_list:
    print(f"  {', '.join(urban_in_list)}")
print(f"Other tiles (~10 MB - 1 GB each): {other_count}")
print(f"\nEstimated total download size:")
print(f"  Minimum: {len(urban_in_list) * 1 + other_count * 0.01:.1f} GB")
print(f"  Average: {len(urban_in_list) * 4 + other_count * 0.2:.1f} GB")
print(f"  Maximum: {len(urban_in_list) * 8 + other_count * 1:.1f} GB")

# ============================================================================
# SUMMARY
# ============================================================================

print(f"\n{'=' * 60}")
print("SUMMARY - FILES CREATED")
print(f"{'=' * 60}")
print(f"1. {tile_list_file}")
print(f"2. {rsync_pattern_file}")
print(f"3. {mapping_file}")

print(f"\n{'=' * 60}")
print("READY TO DOWNLOAD!")
print(f"{'=' * 60}")
print(f"""
Total tiles to download: {len(sorted_tiles)}
Estimated size: {len(urban_in_list) * 4 + other_count * 0.2:.1f} GB (average)

In Cygwin terminal, run:

export RSYNC_PASSWORD='m1782307'

rsync -avP --include-from={rsync_pattern_file} \\
      rsync://m1782307@dataserv.ub.tum.de/m1782307/ \\
      ./building_data/

The download can be interrupted and resumed - just re-run the same command.
""")

print(f"{'=' * 60}")

# ============================================================================
# Explore FTP server structure before downloading
# ============================================================================

FTP_HOST = 'dataserv.ub.tum.de'
FTP_USER = 'm1782307'
FTP_PASS = 'm1782307'

print("Exploring FTP Server Structure...\n")

try:
    ftp = FTP(FTP_HOST, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)

    print("✓ Connected successfully!\n")

    # Get current directory
    print(f"Current directory: {ftp.pwd()}")

    # List root directory
    print("\n--- Root Directory Contents ---")
    files = []
    ftp.retrlines('LIST', files.append)

    for item in files:
        print(item)

    # Save to file
    with open('outputs/ftp_structure.txt', 'w') as f:
        f.write("FTP Server Structure\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Current directory: {ftp.pwd()}\n\n")
        for item in files:
            f.write(f"{item}\n")

    print("\n\nStructure saved to: ftp_structure.txt")
    print("\nPlease check this file to understand how files are organized.")

    ftp.quit()

except Exception as e:
    print(f"Error: {e}")
    print("\nThis suggests:")
    print("1. FTP might be blocked by firewall")
    print("2. Credentials might be incorrect")
    print("3. Server might not allow FTP connections")