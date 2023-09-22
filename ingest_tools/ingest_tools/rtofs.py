import re
import datetime
from typing import Tuple

import fsspec
import ujson
from kerchunk.combine import MultiZarrToZarr

from .generic import generate_kerchunked_nc


def generate_rtofs_output_key(key: str) -> str:
    '''
    Generate the output file key for a given input key and destination bucket and prefix:
        'rtofs.20230922/rtofs_glo_2ds_f001_diag.nc'
    The following output key will be generated: rtofs.20230922.rtofs_glo_2ds_f001_diag.t00z.nc.zarr'
    '''
    components = key.split('/')
    model_date = components[-2]
    filename = components[-1]
    return f'{model_date}.{filename}.zarr'


def generate_kerchunked_rtofs_nc(region: str, bucket: str, key: str, dest_bucket: str, dest_prefix: str):
    '''
    Generate a kerchunked zarr file from a netcdf file in s3
    '''
    filekey = generate_rtofs_output_key(key)
    generate_kerchunked_nc(bucket, key, filekey, dest_bucket, dest_prefix)