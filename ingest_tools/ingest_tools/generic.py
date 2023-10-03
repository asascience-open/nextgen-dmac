import fsspec
import ujson
from kerchunk.hdf import SingleHdf5ToZarr


def generate_kerchunked_nc(bucket: str, key: str, dest_key: str, dest_bucket: str, dest_prefix: str):
    '''
    Generate a kerchunked zarr file from a netcdf file in s3
    '''
    if not key.endswith('.nc'):
        print(f'File {key} does not have a netcdf file postfix. Skipping...')
        return

     # For now SSL false is solving my cert issues **shrug**
    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True, use_ssl=False)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True, use_ssl=False)

    url = f"s3://{bucket}/{key}"
    outurl = f"s3://{dest_bucket}/{dest_prefix}/{dest_key}"

    with fs_read.open(url) as ifile:
        print(f"Kerchunking netcdf at {url}")
        try:
            chunks = SingleHdf5ToZarr(ifile, url)
        except Exception as e:
            print(f'Failed to kerchunk {url}: {e}')
            return

        print(f"Writing kerchunked json to {outurl}")
        with fs_write.open(outurl, mode="w") as ofile:
            data = ujson.dumps(chunks.translate())
            ofile.write(data)
    
    print(f'Successfully processed {url}')