import warnings
from pathlib import Path

import click

from .download_acquisition_kmls import download_all_acquisition_kmls
from .parse_kml import join_grid_acqs


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


main.add_command(download_acquisition_kmls)
main.add_command(join_grid_acquisitions)

if __name__ == '__main__':
    main()
