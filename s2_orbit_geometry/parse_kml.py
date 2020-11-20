from io import BytesIO
from pathlib import Path
from typing import Iterable, List
from urllib.request import urlretrieve
from zipfile import ZipFile

import click
import geopandas as gpd
import pandas as pd
import requests
import shapely
from bs4 import BeautifulSoup
from keplergl_cli import Visualize
from shapely.geometry import MultiPolygon, Polygon

path = '/Users/kyle/github/mapping/s2-orbit-geometry/Sentinel-2A_MP_ACQ_KML_20200728T120000_20200820T150000.kml'
grid_path = '/Users/kyle/github/mapping/s2-orbit-geometry/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml'

# Enable fiona driver
gpd.io.file.fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'


# Read file
def main():
    acq_url = 'https://sentinel.esa.int/documents/247904/4041069/Sentinel-2A_MP_ACQ_KML_20200728T120000_20200820T150000.kml'
    grid_path = '/Users/kyle/github/mapping/s2-orbit-geometry/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.kml'

    acq_gdf = parse_acq_kml(acq_url)
    grid_gdf = load_grid(grid_path)
    joined = join_grid_acq(grid_gdf, acq_gdf)


def join_grid_acq(grid_gdf: gpd.GeoDataFrame,
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
        lambda geom: shapely.ops.transform(lambda x, y, z: (x, y), geom))

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
        lambda polygon: shapely.ops.transform(lambda x, y, z: (x, y), polygon))

    # 'ObservationTimeStart', 'ObservationTimeStop', 'ObservationDuration',
    # 'Scenes'
    keep_cols = ['ID', 'OrbitRelative', 'geometry']
    return gdf[keep_cols]


pd.options.display.max_columns = None



def download_all_acquisition_kmls(out_dir: Path) -> None:
    out_dir = Path(out_dir)
    urls = get_all_acquisition_urls()

    with click.progressbar(urls) as bar:
        for url in bar:
            if url.endswith('.kml'):
                out_path = out_dir / url.split('/')[-1]
                urlretrieve(url, out_path)
                continue

            if url.endswith('.zip'):
                r = requests.get(url)
                with ZipFile(BytesIO(r.content)) as zf:
                    for name in zf.namelist():
                        with zf.open(name) as file:
                            out_path = out_dir / name.split('/')[-1]
                            with open(out_path, 'wb') as f:
                                f.write(file.read())

                continue

            raise ValueError(f'URL must end in .kml or .zip: {url}')


def get_all_acquisition_urls() -> List[str]:
    current = list(get_all_current_acquisition_urls())
    archive = list(get_all_archive_acquisition_urls())

    return [*current, *archive]


def get_all_current_acquisition_urls() -> Iterable[str]:
    baseurl = 'https://sentinel.esa.int'
    url = baseurl + '/web/sentinel/missions/sentinel-2/acquisition-plans'
    r = requests.get(url)
    soup = BeautifulSoup(r.content)

    s2a_kmls = soup.select('.sentinel-2a a')
    s2b_kmls = soup.select('.sentinel-2b a')

    for item in [*s2a_kmls, *s2b_kmls]:
        yield baseurl + item.attrs['href']


def get_all_archive_acquisition_urls() -> Iterable[str]:
    baseurl = 'https://sentinel.esa.int'
    url = baseurl + '/web/sentinel/missions/sentinel-2/acquisition-plans/archive'
    r = requests.get(url)
    soup = BeautifulSoup(r.content)

    s2a_kmls = soup.select('.sentinel-2a a')
    s2b_kmls = soup.select('.sentinel-2b a')

    for item in [*s2a_kmls, *s2b_kmls]:
        yield baseurl + item.attrs['href']

    # Missing from above divs
    yield 'https://sentinel.esa.int/documents/247904/3216744/Sentinel-2B-Acquisition-Plans-2017.zip'
