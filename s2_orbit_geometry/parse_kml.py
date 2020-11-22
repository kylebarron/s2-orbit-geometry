import string
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import click
import geopandas as gpd
import numpy as np
import pandas as pd
import pygeos
from bs4 import BeautifulSoup
from keplergl_cli import Visualize
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.ops import transform

# Enable fiona driver
gpd.io.file.fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'

def intersect_grid_orbits(
    grid_path: Union[Path, str],
    orbit_path: Union[Path, str],
    filter_orbits: Optional[Dict[str, Set[int]]] = None
) -> Iterable[gpd.GeoDataFrame]:
    grid_gdf = load_grid(grid_path)
    orbit_gdf = load_orbit_kml(orbit_path)

    # If filter_orbits exists, keep only tile ids that exist in it
    # Construct a pandas DataFrame with these orbits for later inner joins
    filter_orbits_df = None
    if filter_orbits:
        grid_gdf = grid_gdf[grid_gdf['tile_id'].isin(filter_orbits.keys())]

        filter_orbits_df = pd.concat([
            pd.DataFrame({
                'tile_id': [tile_id] * len(orbits),
                'relative_orbit': orbits})
            for tile_id, orbits in filter_orbits.items()])
        filter_orbits_df['relative_orbit'] = pd.to_numeric(
            filter_orbits_df['relative_orbit'], downcast='unsigned')

    # Iterate over utm_zone, north/south combos
    grid_grouped = grid_gdf.groupby(['utm_zone', 'utm_north'])
    with click.progressbar(grid_grouped, file=sys.stderr) as bar:
        for (utm_zone, utm_north), group_gdf in bar:
            if utm_zone == 1 or utm_zone == 60:
                print(
                    'Not implemented for UTM zones 1 and 60 due to dateline issues'
                )
                continue

            try:
                yield intersect_grid_orbit_single_utm_zone(
                    group_gdf=group_gdf,
                    orbit_gdf=orbit_gdf,
                    utm_zone=utm_zone,
                    utm_north=utm_north,
                    filter_orbits_df=filter_orbits_df)
            except pygeos.GEOSException:
                pass

            # UTM zones 1 and 60 are along the international dateline, and thus
            # total_bounds spans the entire southern hemisphere, leading to
            # invalid values in the reprojection.
            # for stricter_bbox in [box(-180, -90, 0, 90), box(0, -90, 180, 90)]:
            #     grid_orbit_intersection =  intersect_grid_orbit_single_utm_zone(group_gdf=group_gdf, orbit_gdf=orbit_gdf, utm_zone=utm_zone, utm_north=utm_north, stricter_bbox=stricter_bbox)
            # stricter_bbox = box(-180, -90, 0, 90)


def intersect_grid_orbit_single_utm_zone(
        group_gdf: gpd.GeoDataFrame,
        orbit_gdf: gpd.GeoDataFrame,
        utm_zone: int,
        utm_north: bool,
        filter_orbits_df: Optional[pd.DataFrame] = None) -> gpd.GeoDataFrame:

    # Use bounds of gdf instead of a naive UTM bbox because there are some
    # exceptions to the 6 deg width rule, especially around Norway and Svalbard.
    # The MGRS grid follows these exceptions.
    bbox = box(*group_gdf.total_bounds)

    # Keep orbits that intersect this bbox
    local_utm_orbits = orbit_gdf[orbit_gdf.intersects(bbox)]

    # Intersect with bbox. Since this is a global bbox, it's ok for this to be
    # in WGS84.
    local_utm_orbits = local_utm_orbits.set_geometry(
        local_utm_orbits.geometry.intersection(bbox))

    # Reproject to UTM zone
    epsg_code = get_utm_epsg(utm_zone, utm_north)
    local_grid = group_gdf.to_crs(epsg=epsg_code)
    local_utm_orbits = local_utm_orbits.to_crs(epsg=epsg_code)

    # Get swath for each orbit. The swath is 290km, or 145km on each side.
    # cap_style=3 makes a square cap instead of the default round cap.
    local_utm_swaths = local_utm_orbits.set_geometry(
        local_utm_orbits.buffer(145_000, cap_style=3))

    # Intersect the swaths with the grid
    joined = gpd.sjoin(local_grid, local_utm_swaths, op='intersects')

    # Then merge the swath geometries back onto the grid geometries...
    joined = pd.merge(
        joined,
        local_utm_swaths[['geometry']],
        left_on='index_right',
        right_index=True,
        suffixes=('', '_swath'))

    # Then compute the intersection of the grid and swath geometries
    joined = joined.set_geometry(
        joined.geometry.intersection(joined.geometry_swath))

    # Filter on orbits that exist with an inner join
    if filter_orbits_df:
        joined = pd.merge(
            joined,
            filter_orbits_df,
            on=['tile_id', 'relative_orbit'],
            how='inner')

    # Keep selected columns and reproject back to WGS84
    keep_cols = [
        'tile_id', 'geometry', 'utm_zone', 'utm_north', 'relative_orbit']
    return joined[keep_cols].to_crs(epsg=4326)


def get_utm_epsg(utm_zone: int, north: bool) -> str:
    """Get EPSG code for UTM zone
    """
    if north:
        return f'326{utm_zone:02}'

    return f'327{utm_zone:02}'


def join_grid_acqs(grid_path: Path,
                   acq_paths: List[Path]) -> Iterable[gpd.GeoDataFrame]:
    grid_gdf = load_grid(grid_path)

    with click.progressbar(acq_paths, file=sys.stderr) as bar:
        for acq_path in bar:
            acq_gdf = parse_acq_kml(acq_path)
            yield join_single_grid_acq(grid_gdf, acq_gdf)


def join_single_grid_acq(
        grid_gdf: gpd.GeoDataFrame,
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
    joined = pd.merge(
        joined,
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
    grid_gdf.geometry = grid_gdf.geometry.map(to_2d)

    # Coerce GeometryCollection to a MultiPolygon
    # Each GeometryCollection has one or more Polygon geometries plus a Point
    # geometry (presumably centroid), so filter out the Point
    grid_gdf.geometry = grid_gdf.geometry.apply(
        lambda geom_collection: MultiPolygon([
            g for g in geom_collection.geoms
            if isinstance(g, (MultiPolygon, Polygon))]))

    # UTM zone
    grid_gdf['utm_zone'] = grid_gdf['tile_id'].str[:2].astype(np.uint8)

    # True if zone is north of the equator
    grid_gdf['utm_north'] = grid_gdf['tile_id'].str[2].isin(
        list(string.ascii_uppercase[13:]))

    return grid_gdf


def parse_acq_kml(path: str) -> gpd.GeoDataFrame:
    """Parse Sentinel 2 acquisition KML file

    Args:
        - path: Path on disk or URL to KML file
    """
    gdf = gpd.read_file(path, layer='NOMINAL')

    # Drop Z dimension
    gdf.geometry = gdf.geometry.map(to_2d)

    # Try to remove self-intersections
    gdf.geometry = gdf.geometry.buffer(0)

    # 'ObservationTimeStart', 'ObservationTimeStop', 'ObservationDuration',
    # 'Scenes'
    keep_cols = ['ID', 'OrbitRelative', 'geometry']
    return gdf[keep_cols]


def load_orbit_kml(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path, layer='SENTINEL2A ORBIT Ground-Track')

    # Drop Z dimension
    gdf.geometry = gdf.geometry.map(to_2d)

    gdf = gdf.rename(columns={'Relative_Orbit': 'relative_orbit'})
    gdf = gdf[['relative_orbit', 'geometry']]

    gdf['relative_orbit'] = pd.to_numeric(
        gdf['relative_orbit'], downcast='unsigned')

    return gdf


def to_2d(geom):
    return transform(lambda x, y, z=None: (x, y), geom)
