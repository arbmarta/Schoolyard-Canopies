import geopandas as gpd
import matplotlib.pyplot as plt

gdf = gpd.read_file("../outputs/buildings_near_schools.gpkg")

# Ensure WGS84
if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
    gdf = gdf.to_crs(epsg=4326)

# Sample for speed
gdf_plot = gdf.sample(frac=0.05, random_state=1)

fig, ax = plt.subplots(figsize=(10, 10))

gdf_plot.plot(
    ax=ax,
    linewidth=0.3,
    edgecolor="black",
    facecolor="none",
    aspect=None   # <<< THIS FIXES THE ERROR
)

ax.set_title("Buildings Near Schools (5% sample)")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")

plt.show()
