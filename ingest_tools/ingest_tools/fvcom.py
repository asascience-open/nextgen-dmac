from ingest_tools.ingest_tools.pipeline import Pipeline
from .generic import generate_kerchunked_netcdf, generate_nos_output_key


class FVCOM_Pipeline(Pipeline):

    def __init__(self) -> None:
        super().__init__('.nc', ['ngofs2'], 'nos')

    def generate_output_key(self, src_key: str) -> str:
        return generate_nos_output_key(src_key)

    def generate_kerchunk(self, region: str, src_bucket: str, src_key: str, dest_bucket: str, dest_key: str, dest_prefix: str):
        generate_kerchunked_netcdf(src_bucket, src_key, dest_key, dest_bucket, dest_prefix)