from abc import ABC, abstractmethod
from dataclasses import dataclass
import typing

from ingest_tools.filemetadata import FileMetadata
from ingest_tools.generic import generate_kerchunked
from ingest_tools.pipelineconfig import KerchunkPipelineConfig, PipelineConfig
from .filters import key_contains
 

class KerchunkPipeline(ABC):

    def __init__(self, config: KerchunkPipelineConfig) -> None:
        if config is None:
            raise ValueError('Pipeline configuration is missing')
        self.config = config
    
    def accepts(self, key) -> bool:
        # The pipeline must accept the fileformat input
        if not key.endswith(self.config.fileformat):
            print(f'No ingest available for key: {key}')
            return False
        
        # The pipeline's message filters must match
        if key_contains(key, self.config.filters):
            return True
        
        return False
    
    def run(self, src_bucket: str, src_key: str):
        # TODO: More of a listener pattern might work better
        #self.filemetadata = self.read_file_metadata(src_key)
        # status.log(filemetadata)
        output_key = self.generate_kerchunk_output_key(src_key)
        generate_kerchunked(src_bucket, src_key, output_key, self.config.dest_bucket, self.config.dest_prefix)

    @abstractmethod
    def generate_kerchunk_output_key(self, key: str) -> str:
        pass
       

class AggPipeline(ABC):

    def __init__(self, config: PipelineConfig) -> None:
        if config is None:
            raise ValueError('Pipeline configuration is missing')
        self.config = config

    def accepts(self, key) -> bool:
        # The pipeline's message filters must match
        if key_contains(key, self.config.filters):
            return True
        
        return False
    
    @abstractmethod
    def read_file_metadata(self, key: str) -> FileMetadata:
        pass

    @abstractmethod
    def generate_kerchunk(self, bucket: str, key: str):
        pass

    
class PipelineContext():

    def __init__(self) -> None:
        self.pipelines = {}
    
    def add_pipeline(self, name: str, pipeline):
        self.pipelines[name] = pipeline

    def get_matching_pipelines(self, key: str):
        matching = []
        for p in self.pipelines:        
            pipeline = self.pipelines[p]
            if pipeline.accepts(key):
                matching.append(pipeline)
        return matching