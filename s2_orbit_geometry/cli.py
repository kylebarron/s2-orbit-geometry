import json
import warnings
from pathlib import Path

import click
import pandas as pd

from .available_tile_orbits import find_available_orbit_tiles
from .download_acquisition_kmls import download_all_acquisition_kmls
from .download_copernicus import \
  download_copernicus_csvs as _download_copernicus_csvs
from .parse_kml import intersect_grid_orbits, join_grid_acqs


class PathType(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""
    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


@click.group()
def main():
    pass


@click.command()
@click.option(
    '-o',
    '--out-dir',
    type=PathType(file_okay=False, dir_okay=True, writable=True),
    help='Directory to write KML acquisition paths to.',
    required=True)
def download_acquisition_kmls(out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    download_all_acquisition_kmls(out_dir)


@click.command()
@click.option(
    '--grid-path',
    type=PathType(file_okay=True, dir_okay=False, readable=True, exists=True),
    help='Sentinel 2 MGRS grid',
    required=True)
@click.option(
    '-o',
    '--out-dir',
    type=PathType(file_okay=False, dir_okay=True, writable=True),
    help='Directory to write KML acquisition paths to.',
    required=True)
@click.argument('acq_paths', type=PathType(exists=True), nargs=-1)
def join_grid_acquisitions(grid_path, acq_paths, out_dir):
    acq_paths = [Path(x).resolve() for x in acq_paths]
    it = join_grid_acqs(Path(grid_path), acq_paths)

    warnings.filterwarnings(
        'ignore', message='.*initial implementation of Parquet.*')

    for idx, gdf in enumerate(it):
        input_path = acq_paths[idx]
        gdf.to_parquet(out_dir / (input_path.stem + '.parquet'), index=None)


@click.command()
@click.option(
    '--grid-path',
    type=PathType(file_okay=True, dir_okay=False, readable=True, exists=True),
    help='Sentinel 2 MGRS grid',
    required=True)
@click.option(
    '--orbit-path',
    type=PathType(file_okay=True, dir_okay=False, readable=True, exists=True),
    help='Orbit KML',
    required=True)
@click.option(
    '--filter-orbits-path',
    type=PathType(file_okay=True, dir_okay=False, readable=True, exists=True),
    help=
    'JSON file with orbits to include. Keys should be 5-character tile ids and values should be lists of integers representing relative orbits.',
    required=False,
    default=None)
@click.option(
    '-o',
    '--out-path',
    type=PathType(file_okay=True, dir_okay=False, writable=True),
    help='File to write to.',
    required=True)
def join_orbit_grid(grid_path, orbit_path, filter_orbits_path, out_path):

    filter_orbits = None
    if filter_orbits_path:
        with open(filter_orbits_path) as f:
            filter_orbits = json.load(f)

    joined = pd.concat(
        list(
            intersect_grid_orbits(
                grid_path=grid_path,
                orbit_path=orbit_path,
                filter_orbits=filter_orbits)))
    joined.to_parquet(out_path)


@click.command()
@click.option(
    '--index-path',
    type=PathType(file_okay=True, dir_okay=False, readable=True, exists=True),
    help=
    'Sentinel 2 L2 Index file from Google Cloud Storage (https://console.cloud.google.com/storage/browser/_details/gcp-public-data-sentinel-2/L2/index.csv.gz)',
    required=True)
def available_tile_orbits(index_path):
    """Find orbit-tile combinations that exist

    The MGRS grid covers the entire earth, and thus the orbit-MGRS tile
    intersections also cover the entire earth. But Sentinel 2 doesn't take
    images over oceans, and only takes images in its descending orbit. Therefore
    the orbit-tile index can be made much smaller by filtering only on the
    combinations that actually exist in the data.

    This takes the Google Cloud Storage Sentinel 2 index and looks at all the
    PRODUCT_IDs to find the orbit-tile combinations that exist in the archive.
    """
    tile_orbits = find_available_orbit_tiles(index_path)

    # Convert set to list to be json-encodable
    tile_orbits = {k: list(v) for k, v in tile_orbits.items()}
    click.echo(json.dumps(tile_orbits, separators=(',', ':')))


@click.command()
@click.option(
    '--out-dir',
    type=PathType(file_okay=False, dir_okay=True, writable=True),
    help='Directory for writing Copernicus CSVs',
    required=True)
def download_copernicus_csvs(out_dir):
    _download_copernicus_csvs(out_dir)


main.add_command(download_acquisition_kmls)
main.add_command(join_grid_acquisitions)
main.add_command(join_orbit_grid)
main.add_command(available_tile_orbits)
main.add_command(download_copernicus_csvs)

if __name__ == '__main__':
    main()
