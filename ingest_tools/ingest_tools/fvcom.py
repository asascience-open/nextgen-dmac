import re
from ingest_tools.filemetadata import FileMetadata
from ingest_tools.pipeline import Pipeline
from .generic import generate_kerchunked
from .nos_ofs import generate_nos_output_key


class FVCOM_Pipeline(Pipeline):

    def __init__(self) -> None:
        super().__init__('.nc', ['ngofs2'], 'nos')

    # TODO: Integrate with NOS class
    def read_file_metadata(self, key: str) -> FileMetadata:
        # this will be specific per pipeline
        parts = key.split('/')
        model_name = parts[0].split('.')[0]
        model_date, model_hour = re.search(r'(\d{8}).t(\d{2})', key).groups()
        output_key = generate_nos_output_key(key)
        # TODO: offset = 
        return FileMetadata(key, model_name, model_date, model_hour, 0, output_key)

    def generate_kerchunk(self, region: str, src_bucket: str, src_key: str, dest_bucket: str, dest_key: str, dest_prefix: str):
        generate_kerchunked(src_bucket, src_key, dest_key, dest_bucket, dest_prefix)