import re
import datetime
from typing import Tuple

import fsspec
import ujson
from ingest_tools.filemetadata import FileMetadata
from ingest_tools.pipelineconfig import ConfigContext
from ingest_tools.pipeline import AggPipeline, KerchunkPipeline, PipelineConfig
from kerchunk.combine import MultiZarrToZarr


class RTOFS_Pipeline(KerchunkPipeline):
    
    def __init__(self) -> None:
        super().__init__('.nc', ['rtofs'], "s3://dest_bucket", 'rtofs')

    def generate_kerchunk_output_key(self, key: str) -> str:
        '''This should be replaced eventually'''
        components = key.split('/')
        model_date = components[-2]
        filename = components[-1]
        return f'{model_date}.{filename}.zarr'


class RTOFS_Agg_Pipeline(AggPipeline):

    def read_file_metadata(self, key: str) -> FileMetadata:
        '''
        Generate the output file key for a given input key and destination bucket and prefix:
            'rtofs.20230922/rtofs_glo_2ds_f001_diag.nc'
        The following output key will be generated: rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr'
        '''
        components = key.split('/')
        model_date = components[-2]
        filename = components[-1]
        output_key = f'{model_date}.{filename}.zarr'
        model_date_formatted, offset = parse_rtofs_model_run_datestamp_offset(key)        
        return FileMetadata(key, 'rtofs', model_date_formatted, str(offset), offset, output_key)

    def generate_kerchunk(self, bucket: str, key: str):
        generate_kerchunked_rtofs_best_time_series(bucket, key)


def generate_rtofs_best_time_series_glob_expression(key: str) -> str:
    '''
    Parse the glob prefix and postfix given the zarr single file key: 
        'rtofs/rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr'
    The following expression will be created: rtofs/rtofs.*.rtofs_glo_2ds_f*_diag.nc.zarr'
    '''
    prefix, inner, postfix = re.search(r'(.*).\d{8}.(.*)_f\d{3}_(.*)', key).groups()
    return f'{prefix}.*.{inner}_f*_{postfix}'


def parse_rtofs_model_run_datestamp_offset(key: str) -> Tuple[str, int]:
    '''
    Parse the model run forecast time key from the key of the file in the RTOFS S3 bucket, given the RTOFS naming convention: 
        'rtofs/rtofs.20230922.rtofs_glo_2ds_f001_diag.nc.zarr' 
    where the model_date is 20230922 and the offset is 1, this would result in a key of 20230922T01
    '''
    model_date, offset = re.search(r'(\d{8}).*f(\d{3})', key).groups()
    model_date = datetime.datetime.strptime(f'{model_date}T00', '%Y%m%dT%H') + datetime.timedelta(hours=int(offset))
    model_date_key = model_date.strftime('%Y%m%dT%H')
    return model_date_key, int(offset)


def generate_rtofs_best_timeseries_key(best_timeseries_glob: str) -> str:
    '''
    Create the best time series key for a given glob expression:
        'rtofs/rtofs.*.rtofs_glo_2ds_f*_diag.nc.zarr'
    The following key will be generated: rtofs/rtofs.rtofs_glo_2ds_diag.best.nc.zarr'
    '''
    return best_timeseries_glob.replace('.*', '').replace('_f*', '').replace('.nc.zarr', '.best.nc.zarr')


def generate_kerchunked_rtofs_best_time_series(bucket: str, key: str):
    '''
    Generate or update the best time series kerchunked aggregation for the model. If the specified file is not in the best time series, 
    then the best time series aggregation will not be updated
    '''
    print(f'Generating best time series multizarr aggregation for key: {key}')

    try:
        best_time_series_glob = generate_rtofs_best_time_series_glob_expression(key)
    except Exception as e: 
        print(f'Failed to parse model run date and hour from key {key}: {e}. Skipping...')
        return
    
    # For now SSL false is solving my cert issues **shrug**
    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True, use_ssl=False)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True, use_ssl=False)

    model_files = fs_read.glob(f's3://{bucket}/{best_time_series_glob}')
    model_files = sorted(['s3://'+f for f in model_files])

    indexes = {}

    for f in model_files:
        model_date_key, offset = parse_rtofs_model_run_datestamp_offset(f)
        if model_date_key not in indexes:
            indexes[model_date_key] = [offset, f]
        else: 
            if offset < indexes[model_date_key][0]:
                indexes[model_date_key] = [offset, f]

    model_best_files = [x[1] for x in list(indexes.values())]
    
    target_key = f's3://{bucket}/{key}'
    if target_key not in model_best_files:
        print(f'{key} is not a part of the current best time series for its model. Skipping...')
        return

    model_run_file_count = len(model_best_files)
    print(f'Aggregating {model_run_file_count} model files for best time series aggregation...')

    # TODO: Use the values from config object
    mzz = MultiZarrToZarr(
        model_best_files, 
        remote_protocol='s3', 
        remote_options={'anon': True, 'use_ssl': False},
        concat_dims=['MT'],
        identical_dims=['Y', 'X', 'Latitude', 'Longitude']
    )

    d = mzz.translate()

    outkey = generate_rtofs_best_timeseries_key(best_time_series_glob)
    outurl = f's3://{bucket}/{outkey}'

    print(f'Writing zarr best time series aggregation to {outurl}')
    with fs_write.open(outurl, 'w') as ofile:
        ofile.write(ujson.dumps(d))
    
    print(f'Successfully updated {outurl} RTOFS best time series aggregation')