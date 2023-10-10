
from dataclasses import dataclass


@dataclass
class FileMetadata():

    source_key: str
    dataset_id: str
    model_date: str
    model_hour: str
    offset: int
    output_key: str