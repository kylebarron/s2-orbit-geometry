from io import BytesIO
from pathlib import Path
from typing import Iterable, List
from urllib.request import urlretrieve
from zipfile import ZipFile

import click
import requests
from bs4 import BeautifulSoup


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
