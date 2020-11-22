"""
Match Sentinel 2 metadata from GCP and AWS

GCP helpfully has a bulk metadata file with all the PRODUCT_IDs. AWS' Sentinel 2
cogs bucket doesn't currently have a bulk metadata file, except for the S3
inventory bucket that contains a list of files in the bucket
"""
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Union, Iterable
def load_aws_metadata(
        aws_index_paths: Iterable[Union[str, Path]]) -> Iterable[pd.DataFrame]:
    for aws_index_path in aws_index_paths:
        aws_df_iter = pd.read_csv(
            aws_path,
            names=['bucket', 'key', 'size', 'last_modified'],
            chunksize=100_000,
            dtype={
                'bucket': str,
                'key': str,
                'size': np.uint32},
            # Only care about path
            usecols=['key'])

        for aws_df in aws_df_iter:
            # Split key into paths
            aws_df['split_key'] = aws_df['key'].str.split('/')

            # Keep if second folder is only two digits long
            # There are some older files in the bucket of format
            # `sentinel-s2-l2a-cogs/[YEAR]` that we want to avoid
            aws_df = aws_df[aws_df['split_key'].apply(
                lambda row: len(row[1]) == 2)]

            # Grab just the id part
            aws_id = pd.DataFrame(
                aws_df['split_key'].apply(lambda row: row[6]).rename(
                    's3_id')).set_index('s3_id')

            # Drop duplicates
            # NOTE there can still be duplicates across chunks!!
            aws_id = aws_id[~aws_id.index.duplicated()]

            yield aws_id


def load_google_metadata(gcp_index_path: Union[str, Path]) -> pd.DataFrame:
    keep_cols = ['PRODUCT_ID', 'MGRS_TILE', 'SENSING_TIME', 'CLOUD_COVER']
    # Use iterator to minimize intermediate memory usage
    gcp_df_iter = pd.read_csv(
        gcp_index_path,
        chunksize=100_000,
        parse_dates=['SENSING_TIME'],
        usecols=keep_cols)

    chunks = []
    for gcp_df_chunk in gcp_df_iter:
        # Keep only L1C
        # Use contains instead of beginning of string for some old product ids
        # with longer format
        gcp_df_chunk = gcp_df_chunk[gcp_df_chunk['PRODUCT_ID'].str.contains(
            'MSIL1C')]
        chunks.append(gcp_df_chunk)

    gcp_df = pd.concat(chunks)

    # Make cols lower case
    gcp_df = gcp_df.rename(mapper=str.lower, axis=1)

    # Construct sensing _day_ from sensing _time_
    gcp_df['sensing_day'] = pd.to_datetime({
        'year': gcp_df['sensing_time'].dt.year,
        'month': gcp_df['sensing_time'].dt.month,
        'day': gcp_df['sensing_time'].dt.day})

    # Sort time in ascending order
    gcp_df = gcp_df.sort_values('sensing_time', ascending=True)

    # Find # collect per tile per day
    gcp_df['datatake_order'] = gcp_df.groupby(['mgrs_tile', 'sensing_day'],
                                              sort=False).cumcount()

    # Create s3 id
    gcp_df['s3_id'] = (
        gcp_df['product_id'].str[:3]
        + '_'
        + gcp_df['mgrs_tile']
        + '_'
        + gcp_df['sensing_day'].dt.strftime('%Y%m%d')
        + '_'
        + gcp_df['datatake_order'].astype(str)
        + '_'
        + 'L2A') # yapf: disable
    gcp_df = gcp_df.set_index('s3_id')

    return gcp_df


# inner join with s3 metadata
# merge geometries
