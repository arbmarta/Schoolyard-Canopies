import geopandas as gpd

gdf = gpd.read_file("buildings_near_schools.gpkg", layer="buildings")

dups = gdf.duplicated(subset="geometry").sum()
print("Exact duplicate geometries:", dups)