import fsspec
import ujson
from kerchunk.hdf import SingleHdf5ToZarr


def generate_kerchunked_ofs_aws(region: str, bucket: str, key: str): 
    print(f"region: {region}, bucket: {bucket}, key: {key}")
