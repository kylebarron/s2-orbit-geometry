import numpy as np
import string
import sys
from pathlib import Path
from typing import Iterable, List

import click
import geopandas as gpd
import pandas as pd
from bs4 import BeautifulSoup
from keplergl_cli import Visualize
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform

# Enable fiona driver
gpd.io.file.fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'

def intersect_grid_orbit(group_gdf: gpd.GeoDataFrame, orbit_gdf: gpd.GeoDataFrame, utm_zone: int, utm_north: bool):

    # Use bounds of gdf instead of a naive UTM bbox because there are some
    # exceptions to the 6 deg width rule, especially around Norway and Svalbard.
    # The MGRS grid follows these exceptions.
    bbox = box(*group_gdf.total_bounds)

    # Keep orbits that intersect this bbox
    local_utm_orbits = orbit_gdf[orbit_gdf.intersects(bbox)]

    # Intersect with bbox. Since this is a global bbox, it's ok for this to be
    # in WGS84.
    local_utm_orbits = local_utm_orbits.set_geometry(local_utm_orbits.geometry.intersection(bbox))

    # Reproject to UTM zone
    epsg_code = get_utm_epsg(utm_zone, utm_north)
    local_grid = group_gdf.to_crs(epsg=epsg_code)
    local_utm_orbits = local_utm_orbits.to_crs(epsg=epsg_code)

    # Get swath for each orbit. The swath is 290km, or 145km on each side.
    # cap_style=3 makes a square cap instead of the default round cap.
    local_utm_swaths = local_utm_orbits.set_geometry(local_utm_orbits.buffer(145_000, cap_style=3))

    # Intersect the swaths with the grid
    joined = gpd.sjoin(local_grid, local_utm_swaths, op='intersects')

    # Then merge the swath geometries back onto the grid geometries...
    joined = pd.merge(joined,
                      local_utm_swaths,
                      left_on='index_right',
                      right_index=True,
                      suffixes=('', '_swath'))

    # Then compute the intersection of the grid and swath geometries
    joined = joined.set_geometry(joined.geometry.intersection(joined.geometry_swath))

    # Keep selected columns and reproject back to WGS84
    keep_cols = ['tile_id', 'geometry', 'utm_zone', 'utm_north', 'relative_orbit']
    return joined[keep_cols].to_crs(epsg=4326)



def join_grid_acqs(grid_path: Path,
                   acq_paths: List[Path]) -> Iterable[gpd.GeoDataFrame]:
    grid_gdf = load_grid(grid_path)

    with click.progressbar(acq_paths, file=sys.stderr) as bar:
        for acq_path in bar:
            acq_gdf = parse_acq_kml(acq_path)
            yield join_single_grid_acq(grid_gdf, acq_gdf)


def join_single_grid_acq(grid_gdf: gpd.GeoDataFrame,
                         acq_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Join MGRS grid and acquisition paths

    This spatially joins the two datasets on intersecting geometries, then
    modifies the output geometries to be those intersections.
    """
    # Do a spatial join to find the grid tiles that intersect each acquisition
    # path. Since the grid is `left`, the output has the same geometries as the
    # input grid_gdf.
    joined = gpd.sjoin(grid_gdf, acq_gdf, op='intersects')

    # Then merge the acquisition paths back onto the grid geometries...
    joined = pd.merge(joined,
                      acq_gdf,
                      left_on='index_right',
                      right_index=True,
                      suffixes=('', '_acq'))

    # Then compute the intersection of the grid and acquisition geometries
    joined.geometry = joined.geometry.intersection(joined.geometry_acq)

    keep_cols = ['tile_id', 'geometry', 'OrbitRelative']
    return joined[keep_cols]

    # stac_path = '../S2A_12SWF_20201029_0_L2A.json'
    # import json
    # with open(stac_path) as f:
    #     stac = json.load(f)

    # Visualize([merged[(merged['tile_id'] == '12SWF') & (merged['OrbitRelative'] == '84')][['geometry', 'tile_id', 'OrbitRelative']], stac])


def load_grid(grid_path: str) -> gpd.GeoDataFrame:
    """Load Sentinel's KML MGRS grid into GeoDataFrame

    Args:
        - grid_path: Path on disk to KML MGRS grid.
    """
    grid_gdf = gpd.read_file(grid_path, layer='Features')

    # Keep specific columns
    grid_gdf = grid_gdf.rename(columns={'Name': 'tile_id'})
    grid_gdf = grid_gdf[['tile_id', 'geometry']]

    # Coerce to 2D
    grid_gdf.geometry = grid_gdf.geometry.map(
        lambda geom: transform(lambda x, y, z: (x, y), geom))

    # Coerce GeometryCollection to a MultiPolygon
    # Each GeometryCollection has one or more Polygon geometries plus a Point
    # geometry (presumably centroid), so filter out the Point
    grid_gdf.geometry = grid_gdf.geometry.apply(
        lambda geom_collection: MultiPolygon([
            g for g in geom_collection.geoms
            if isinstance(g, (MultiPolygon, Polygon))
        ]))

    return grid_gdf


def parse_acq_kml(path: str) -> gpd.GeoDataFrame:
    """Parse Sentinel 2 acquisition KML file

    Args:
        - path: Path on disk or URL to KML file
    """
    gdf = gpd.read_file(path, layer='NOMINAL')

    # Drop Z dimension
    gdf.geometry = gdf.geometry.map(
        lambda polygon: transform(lambda x, y, z: (x, y), polygon))

    # Try to remove self-intersections
    gdf.geometry = gdf.geometry.buffer(0)

    # 'ObservationTimeStart', 'ObservationTimeStop', 'ObservationDuration',
    # 'Scenes'
    keep_cols = ['ID', 'OrbitRelative', 'geometry']
    return gdf[keep_cols]
