"""
Identify census blocks that contain schools through spatial intersection.

Temporality: 2019 - 2021 (Canada), 2019 - 2020 (United States)
CRS:
- Canada: EPSG:3347 (Statistics Canada Lambert)
- United States: EPSG:5070 (NAD83 / Conus Albers)
Both are equal-area projections in meters
"""

import geopandas as gpd
from pathlib import Path
import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

COUNTRIES = {
    'canada': {
        'epsg': 3347,
        'crs': 'EPSG:3347',
        'name': 'Statistics Canada Lambert',
        'schools_path': '../inputs/schools/canada/canada_school_points.gpkg',
        'schools_layer': 'schools',
        'census_path': '../inputs/census_blocks/canada/census_boundary_files/ldb_000b21a_e.shp',
        'output_path': '../inputs/census_blocks/canada/Canada_census_blocks_with_schools.gpkg',
        'is_directory': False,
        'geoid_column': None
    },
    'united_states': {
        'epsg': 5070,
        'crs': 'EPSG:5070',
        'name': 'NAD83 Conus Albers',
        'schools_path': '../inputs/schools/united_states/us_school_points.gpkg',
        'schools_layer': 'schools',
        'census_path': '../inputs/census_blocks/united_states/state_level',
        'output_path': '../inputs/census_blocks/united_states/us_census_blocks_with_schools.gpkg',
        'is_directory': True,
        'geoid_column': 'GEOID20'
    }
}


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_country(country_name, config):
    """Process census blocks and schools for a country."""

    print("=" * 60)
    print(f"{country_name.upper()}: Census Blocks & Schools Intersection")
    print(f"Using {config['crs']} ({config['name']}) - meters")
    print("=" * 60)

    # ========================================================================
    # PART 1: Load school points
    # ========================================================================
    print("\n[PART 1] Loading school points...")

    schools_path = Path(config['schools_path'])
    if not schools_path.exists():
        print(f"ERROR: School points file not found at {schools_path}")
        print("Please run 01_school_records_to_points.py first")
        return False

    schools_gdf = gpd.read_file(schools_path, layer=config['schools_layer'])
    print(f"Loaded {len(schools_gdf)} school points")
    print(f"CRS: {schools_gdf.crs}")

    # ========================================================================
    # PART 2: Check if output already exists
    # ========================================================================
    output_path = Path(config['output_path'])

    if output_path.exists():
        print(f"\n[PART 2] Found existing processed census blocks file!")
        print(f"Loading from: {output_path}")
        master_gdf = gpd.read_file(output_path, layer='census_blocks')
        print(f"Loaded {len(master_gdf)} census blocks with schools")
        print(f"CRS: {master_gdf.crs}")
        print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")
        final_count = len(master_gdf)

    else:
        # ====================================================================
        # PART 2: Load census blocks
        # ====================================================================
        print("\n[PART 2] No processed file found - will process census blocks...")

        census_path = Path(config['census_path'])

        if config['is_directory']:
            # US: Multiple shapefiles in directory
            print("Loading census block shapefiles from directory...")
            census_shps = list(census_path.rglob('*.shp'))
            print(f"Found {len(census_shps)} census block shapefiles")

            if len(census_shps) == 0:
                print("ERROR: No census block shapefiles found!")
                return False

            # Process each shapefile
            print("\n[PART 3] Filtering census blocks by school intersection...")
            filtered_blocks = []

            for i, shp_file in enumerate(census_shps, 1):
                try:
                    print(f"\nProcessing ({i}/{len(census_shps)}): {shp_file.name}")
                    census_gdf = gpd.read_file(shp_file)

                    # Reproject if needed
                    if census_gdf.crs.to_epsg() != config['epsg']:
                        census_gdf = census_gdf.to_crs(config['crs'])

                    print(f"  Total blocks in file: {len(census_gdf)}")

                    # Spatial join
                    intersecting = gpd.sjoin(
                        census_gdf,
                        schools_gdf,
                        how='inner',
                        predicate='intersects'
                    )

                    # Get unique census blocks
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
                return False

            # Concatenate all blocks
            master_gdf = pd.concat(filtered_blocks, ignore_index=True)
            print(f"\nTotal census blocks with schools: {len(master_gdf)}")

            # Remove duplicates if GEOID column exists
            if config['geoid_column']:
                print(f"\nRemoving duplicates based on {config['geoid_column']}...")
                initial_count = len(master_gdf)
                master_gdf = master_gdf.drop_duplicates(subset=[config['geoid_column']], keep='first')
                final_count = len(master_gdf)
                duplicates_removed = initial_count - final_count

                if duplicates_removed > 0:
                    print(f"Removed {duplicates_removed} duplicate census blocks")
                else:
                    print("No duplicate census blocks found")
            else:
                final_count = len(master_gdf)

            # Count schools outside census blocks (need to reload all census blocks)
            print(f"\nCounting schools outside census blocks...")
            all_census_blocks = []
            for shp_file in census_shps:
                try:
                    census_gdf = gpd.read_file(shp_file)
                    if census_gdf.crs.to_epsg() != config['epsg']:
                        census_gdf = census_gdf.to_crs(config['crs'])
                    all_census_blocks.append(census_gdf)
                except Exception as e:
                    continue

            all_census = pd.concat(all_census_blocks, ignore_index=True)
            schools_in_blocks = gpd.sjoin(
                schools_gdf,
                all_census,
                how='left',
                predicate='within'
            )
            schools_outside = schools_in_blocks[schools_in_blocks.index_right.isna()]
            print(f"Schools outside census blocks: {len(schools_outside)}")

        else:
            # Canada: Single shapefile
            print(f"Loading census blocks from: {census_path.name}")

            if not census_path.exists():
                print(f"ERROR: Census block shapefile not found at {census_path}")
                return False

            census_gdf = gpd.read_file(census_path)
            print(f"Total census blocks: {len(census_gdf)}")

            # Reproject if needed
            if census_gdf.crs.to_epsg() != config['epsg']:
                print(f"Reprojecting to {config['crs']}...")
                census_gdf = census_gdf.to_crs(config['crs'])

            # Spatial join
            print("\n[PART 3] Performing spatial intersection...")
            intersecting = gpd.sjoin(
                census_gdf,
                schools_gdf,
                how='inner',
                predicate='intersects'
            )

            print(f"Total intersections found: {len(intersecting)}")

            # Get unique census blocks
            unique_block_indices = intersecting.index.unique()
            master_gdf = census_gdf.loc[unique_block_indices].copy()

            print(f"Census blocks containing schools: {len(master_gdf)}")

            # Count schools outside census blocks
            schools_in_blocks = gpd.sjoin(
                schools_gdf,
                census_gdf,
                how='left',
                predicate='within'
            )
            schools_outside = schools_in_blocks[schools_in_blocks.index_right.isna()]
            print(f"Schools outside census blocks: {len(schools_outside)}")

            final_count = len(master_gdf)

            if final_count == 0:
                print("WARNING: No census blocks with schools found!")
                return False

        # ====================================================================
        # PART 4: Save filtered census blocks
        # ====================================================================
        print("\n[PART 4] Saving filtered census blocks...")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        master_gdf.to_file(output_path, driver='GPKG', layer='census_blocks')
        print(f"Saved to: {output_path}")
        print(f"File size: {output_path.stat().st_size / (1024 ** 2):.2f} MB")

    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print(f"\n{'=' * 60}")
    print(f"SUCCESS!")
    print(f"{'=' * 60}")
    print(f"Output file: {output_path}")
    print(f"Total schools processed: {len(schools_gdf)}")
    print(f"Total census blocks with schools: {final_count}")
    print(f"CRS: {master_gdf.crs}")
    print(f"EPSG: {config['epsg']}")
    print(f"Units: meters (equal-area projection)")
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