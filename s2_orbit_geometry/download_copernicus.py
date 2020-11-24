import sys
from pathlib import Path
from urllib.request import urlretrieve

import click
import requests
from bs4 import BeautifulSoup


def download_copernicus_csvs(out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)

    s2a_url = 'https://scihub.copernicus.eu/catalogueview/S2A'
    s2b_url = 'https://scihub.copernicus.eu/catalogueview/S2B'

    print('Finding all CSV urls. This may take a while.', file=sys.stderr)
    csv_urls = [
        *find_copernicus_csv_urls(s2a_url), *find_copernicus_csv_urls(s2b_url)]

    with click.progressbar(csv_urls, file=sys.stderr) as bar:
        for csv_url in bar:
            out_path = out_dir / Path(csv_url).name
            urlretrieve(csv_url, out_path)


def find_copernicus_csv_urls(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')
    leaves = find_leaf_hrefs(soup)
    for leaf in leaves:
        new_url = f"{url.rstrip('/')}/{leaf}"
        if leaf.endswith('.csv'):
            yield new_url
            continue

        yield from find_copernicus_csv_urls(new_url)


def find_leaf_hrefs(soup):
    all_links = soup.find_all('a')

    parent_dir_idx = [
        ind for ind, link in enumerate(all_links)
        if link.text == 'Parent Directory']
    assert len(parent_dir_idx) == 1, 'Multiple links to parent directory'

    # Keep all links after Parent Directory link
    # This assumes that all following links on the page are valid down the tree
    all_links = all_links[parent_dir_idx[0] + 1:]

    for link in all_links:
        yield link.attrs['href']
