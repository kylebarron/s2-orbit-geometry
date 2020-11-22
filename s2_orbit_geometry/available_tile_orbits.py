"""
Find available tile orbits from Google Index
"""
from pathlib import Path
from typing import Dict, Set, Union

import numpy as np
import pandas as pd


def find_available_orbit_tiles(
        index_path: Union[Path, str]) -> Dict[str, Set[int]]:
    """Find orbit-tile combinations that exist

    The MGRS grid covers the entire earth, and thus the orbit-MGRS tile
    intersections also cover the entire earth. But Sentinel 2 doesn't take
    images over oceans, and only takes images in its descending orbit. Therefore
    the orbit-tile index can be made much smaller by filtering only on the
    combinations that actually exist in the data.

    This takes the Google Cloud Storage Sentinel 2 index and looks at all the
    PRODUCT_IDs to find the orbit-tile combinations that exist in the archive.
    """
    tile_orbits: Dict[str, Set[int]] = {}

    # regex for finding orbit from product id
    orbit_regex = r'_R(?P<orbit>\d{3})_'

    for df in pd.read_csv(index_path, chunksize=100_000):
        # Make column names lower case
        df = df.rename(mapper=str.lower, axis=1)

        # extract orbit from product id
        df['orbit'] = df['product_id'].str.extract(orbit_regex)['orbit'].astype(
            np.uint8)

        # Keep cols
        df = df[['mgrs_tile', 'orbit']]
        df = df.drop_duplicates()

        for row in df.itertuples():
            tile_orbits[row.mgrs_tile] = tile_orbits.get(row.mgrs_tile, set())
            tile_orbits[row.mgrs_tile].add(row.orbit)

    return tile_orbits
