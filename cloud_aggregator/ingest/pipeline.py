from abc import ABC, abstractmethod
import typing

from ingest_tools.ingest_tools.filters import key_contains


class PipelineContext():

    def __init__(self, dest_bucket: str, dest_prefix: str) -> None:
        self.dest_bucket = dest_bucket
        self.dest_prefix = dest_prefix
        self.pipelines = {}

    def get_dest_bucket(self) -> str:
        return self.dest_bucket
    
    def get_dest_prefix(self) -> str:
        return self.dest_prefix
    
    def add_pipeline(self, name: str, pipeline):
        self.pipelines[name] = pipeline

    def run(self, bucket: str, key: str):        
        for p in self.pipelines:        
            pipeline = self.pipelines[p]
            if pipeline.accepts(key):
                pipeline.run(bucket, key)


class Pipeline(ABC):

    def __init__(self, fileformat: str, filters: typing.List[str]) -> None:
        self.fileformat = fileformat
        self.filters = filters
    
    def accepts(self, key) -> bool:
        # The pipeline must accept the fileformat input
        if not key.endswith(self.fileformat):
            print(f'No ingest available for key: {key}')
            return False
        
        # The pipeline's message filters must match
        if key_contains(key, self.filters):
            return True
        
        return False
    
    def run(self, src_bucket: str, src_key: str, dest_bucket: str, dest_prefix: str):
        dest_key = self.generate_output_key(src_key)
        self.generate_kerchunk(src_bucket, src_key, dest_key, dest_bucket, dest_prefix)
    
    @abstractmethod
    def generate_output_key(self, src_key: str):
        pass

    @abstractmethod
    def generate_kerchunk(self, src_bucket: str, src_key: str, dest_bucket: str, dest_prefix: str):
        pass