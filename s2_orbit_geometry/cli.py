from pathlib import Path

import click

from .download_acquisition_kmls import download_all_acquisition_kmls


@click.group()
def main():
    pass


@click.command()
@click.option('-o',
              '--out-dir',
              type=click.Path(file_okay=False, dir_okay=True, writable=True),
              help='Directory to write KML acquisition paths to.')
def download_acquisition_kmls(out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    download_all_acquisition_kmls(out_dir)


main.add_command(download_acquisition_kmls)

if __name__ == '__main__':
    main()
