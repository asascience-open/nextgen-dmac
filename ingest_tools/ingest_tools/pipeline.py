from abc import ABC, abstractmethod
import typing
from .filters import key_contains


class PipelineContext():

    def __init__(self, region: str, dest_bucket: str) -> None:
        self.region = region
        self.dest_bucket = dest_bucket
        self.pipelines = {}

    def get_region(self) -> str:
        return self.region
    
    def get_dest_bucket(self) -> str:
        return self.dest_bucket
    
    def add_pipeline(self, name: str, pipeline):
        self.pipelines[name] = pipeline

    def run(self, src_bucket: str, key: str):        
        for p in self.pipelines:        
            pipeline = self.pipelines[p]
            if pipeline.accepts(key):
                pipeline.run(self, src_bucket, key)


class Pipeline(ABC):

    def __init__(self, fileformat: str, filters: typing.List[str], dest_prefix: str) -> None:
        self.fileformat = fileformat
        self.filters = filters
        self.dest_prefix = dest_prefix
    
    def accepts(self, key) -> bool:
        # The pipeline must accept the fileformat input
        if not key.endswith(self.fileformat):
            print(f'No ingest available for key: {key}')
            return False
        
        # The pipeline's message filters must match
        if key_contains(key, self.filters):
            return True
        
        return False
    
    def run(self, context: PipelineContext, src_bucket: str, src_key: str):
        dest_key = self.generate_output_key(src_key)
        self.generate_kerchunk(src_bucket, src_key, dest_key, context.get_dest_bucket(), self.dest_prefix)
    
    @abstractmethod
    def generate_output_key(self, src_key: str) -> str:
        pass

    @abstractmethod
    def generate_kerchunk(self, region: str, src_bucket: str, src_key: str, dest_bucket: str, dest_key: str, dest_prefix: str):
        pass