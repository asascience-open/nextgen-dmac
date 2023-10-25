from enum import Enum
from typing import Any, List
import fsspec
import ujson
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr


class ModelRunType(Enum):
    FORECAST = 1
    NOWCAST = 2
    UNKNOWN = 255

    @staticmethod
    def from_offset_prefix(prefix: str):
        if prefix == 'f':
            return ModelRunType.FORECAST
        elif prefix == 'n':
            return ModelRunType.NOWCAST
        else:
            return ModelRunType.UNKNOWN


class FileFormat(Enum):
    NETCDF = 1
    NETCDF_64BIT = 2
    HDF = 3
    GRIB2 = 4
    UNKNOWN = 255

    @staticmethod
    def from_startbytes(raw: bytes):
        '''
        Determine the file format from the first 4 bytes of the file
        '''
        if raw[0:3] == b'CDF':
            if raw[3] == 1:
                return FileFormat.NETCDF
            elif raw[3] == 2:
                return FileFormat.NETCDF_64BIT
        elif raw[0:3] == b'\x89HDF':
            return FileFormat.HDF
        elif raw[0:4] == b'GRIB':
            return FileFormat.GRIB2
        else:
            return FileFormat.UNKNOWN


def generate_kerchunked(bucket: str, key: str, dest_key: str, dest_bucket: str, dest_prefix: str):
    '''
    Generate a kerchunked zarr file from a file in s3

    Automatically determines the file format and uses the appropriate kerchunker processor
    '''
    if not key.endswith('.nc'):
        print(f'File {key} does not have a netcdf file postfix. Skipping...')
        return

    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True)

    url = f"s3://{bucket}/{key}"
    outurl = f"s3://{dest_bucket}/{dest_prefix}/{dest_key}"

    with fs_read.open(url) as ifile:
        print(f'Identifying file at {url}')
        raw = ifile.read(5)
        fmt = FileFormat.from_startbytes(raw)

        if fmt == FileFormat.UNKNOWN or fmt == FileFormat.GRIB2:
            print(f'File format {fmt} for {url} not supported. Skipping...')
            return

        print(f'Kerchunking {url}...')
        try:
            if fmt == FileFormat.NETCDF or fmt == FileFormat.NETCDF_64BIT:
                chunks = NetCDF3ToZarr(url, storage_options={'anon': True})
            elif fmt == FileFormat.HDF:
                chunks = SingleHdf5ToZarr(ifile, url)
        except Exception as e:
            print(f'Failed to kerchunk {url}: {e}')
            return

        print(f"Writing kerchunked json to {outurl}")
        with fs_write.open(outurl, mode="w") as ofile:
            data = ujson.dumps(chunks.translate())
            ofile.write(data)
    
    print(f'Successfully processed {url}')
