import re
import datetime
from typing import List, Tuple

import fsspec
import ujson
from ingest_tools.pipeline import Pipeline, AggPipeline
from ingest_tools.filemetadata import FileMetadata
from kerchunk.combine import MultiZarrToZarr

from .generic import ModelRunType, generate_kerchunked


class NOS_Pipeline(Pipeline):

    def __init__(self) -> None:
        super().__init__('.nc', ['cbofs', 'ciofs', 'creofs', 'dbofs', 'gomofs', 'leofs', 'lmhofs', 'loofs', 'lsofs', 'ngofs2', 'sfbofs', 'tbofs', 'wcofs'], 'nos')

    def read_file_metadata(self, key: str) -> FileMetadata:
        pass

    def generate_kerchunk_output_key(self, key: str) -> str:
        '''This should be replaced eventually'''
        parts = key.split('/')
        model_name = parts[0].split('.')[0]
        return f'{model_name}/{parts[1]}.zarr'

    def generate_kerchunk(self, region: str, src_bucket: str, src_key: str, dest_bucket: str, dest_key: str, dest_prefix: str):
        generate_kerchunked(src_bucket, src_key, dest_key, dest_bucket, dest_prefix)


class ROMS_Agg_Pipeline(AggPipeline):

    def __init__(self) -> None:
        super().__init__(["cbofs", "ciofs", "dbofs", 'gomofs', "tbofs", "wcofs"])

    def read_file_metadata(self, key: str) -> FileMetadata:
        # this will be specific per pipeline

        '''in this case, the keys must come from the NewOFSObject notifications, looking like:
           cbofs.20231022/nos.cbofs.fields.n006.20231022.t00z.nc
           TODO: Standardize/clean key inputs, assert what our assumptions are
        '''
        parts = key.split('/')
        model_name = parts[0].split('.')[0]
        output_key = f'{model_name}/{parts[1]}.zarr'
        output_key = self.generate_kerchunk_output_key(output_key)
        offset, model_date, model_hour = re.search(r'[f|n](\d{3}).(\d{8}).t(\d{2})', key).groups()
        
        return FileMetadata(key, model_name, model_date, model_hour, int(offset), output_key)

    def generate_kerchunk(self, bucket: str, key: str):
        self.generate_kerchunked_nos_roms_model_run(bucket, key)
        self.generate_kerchunked_nos_roms_best_time_series(bucket, key)
    
    def generate_kerchunked_nos_roms_model_run(bucket: str, key: str):
        '''
        Generate or update the multizarr kerchunked aggregation for the model run that the specified file belongs to
        '''
        generate_kerchunked_nos_model_run(
            bucket=bucket, 
            key=key, 
            concat_dims=['ocean_time'], 
            identical_dims=[
                'eta_rho', 
                'xi_rho', 
                's_rho', 
                'eta_psi', 
                'xi_psi', 
                's_w', 
                'eta_u', 
                'xi_u', 
                'eta_v', 
                'xi_v', 
                'lat_rho', 
                'lat_psi', 
                'lat_u', 
                'lat_v', 
                'lon_rho', 
                'lon_psi', 
                'lon_u', 
                'lon_v'
            ])
                
    def generate_kerchunked_nos_roms_best_time_series(bucket: str, key: str):
        '''
        Generate or update the best time series kerchunked aggregation for the model run. If the specified file is not in the best time series, 
        then the best time series aggregation will not be updated
        '''
        generate_kerchunked_nos_best_time_series(
            bucket=bucket,
            key=key,
            concat_dims=['ocean_time'],
            identical_dims=[ # TODO: These are the same as above, easily configurable
                'eta_rho', 
                'xi_rho', 
                's_rho', 
                'eta_psi', 
                'xi_psi', 
                's_w', 
                'eta_u', 
                'xi_u', 
                'eta_v', 
                'xi_v', 
                'lat_rho', 
                'lat_psi', 
                'lat_u', 
                'lat_v', 
                'lon_rho', 
                'lon_psi', 
                'lon_u', 
                'lon_v'
            ]
        )


class FVCOM_Agg_Pipeline(AggPipeline):
    def __init__(self) -> None:
        super().__init__(["leofs", "lmhofs", "loofs", 'lsofs', "ngofs2", "sfbofs"])

    def generate_kerchunk(self, bucket: str, key: str):
        self.generate_kerchunked_nos_fvcom_model_run(bucket, key)
        self.generate_kerchunked_nos_fvcom_best_time_series(bucket, key)

    
    def generate_kerchunked_nos_fvcom_model_run(bucket: str, key: str):
        '''
        Generate or update the multizarr kerchunked aggregation for the model run that the specified file belongs to
        '''
        generate_kerchunked_nos_model_run(
            bucket=bucket,
            key=key,
            concat_dims=['time'],
            identical_dims=['lon', 'lat', 'lonc', 'latc', 'siglay', 'siglev', 'nele', 'node']
        )

    def generate_kerchunked_nos_fvcom_best_time_series(bucket: str, key: str):
        '''
        Generate or update the best time series kerchunked aggregation for the model run. If the specified file is not in the best time series, 
        then the best time series aggregation will not be updated
        '''
        generate_kerchunked_nos_best_time_series(
            bucket=bucket,
            key=key,
            concat_dims=['time'],
            identical_dims=['lon', 'lat', 'lonc', 'latc', 'siglay', 'siglev', 'nele', 'node']
        )


class SELFE_Agg_Pipeline(AggPipeline):

    def __init__(self) -> None:
        super().__init__(['creofs'])

    def generate_kerchunk(self, bucket: str, key: str):
        self.generate_kerchunked_nos_selfe_model_run(bucket, key)
        self.generate_kerchunked_nos_selfe_best_time_series(bucket, key)

    def generate_kerchunked_nos_selfe_model_run(bucket: str, key: str):
        '''
        Generate or update the multizarr kerchunked aggregation for the model run that the specified file belongs to
        '''
        generate_kerchunked_nos_model_run(
            bucket=bucket,
            key=key,
            concat_dims=['time'],
            identical_dims=['lon', 'lat', 'sigma']
        )

    def generate_kerchunked_nos_selfe_best_time_series(region: str, bucket: str, key: str):
        '''
        Generate or update the best time series kerchunked aggregation for the model run. If the specified file is not in the best time series, 
        then the best time series aggregation will not be updated
        '''
        generate_kerchunked_nos_best_time_series(
            bucket=bucket,
            key=key,
            concat_dims=['time'],
            identical_dims=['lon', 'lat', 'sigma']
        )
        

def parse_nos_model_run_datestamp(key: str) -> Tuple[str, str]:
    '''
    Parse the model run date from the key of the file in the NOS S3 bucket, given the NOS naming convention: 
        'nos/dbofs/nos.dbofs.fields.f001.20230315.t00z.nc.zarr' 
    where the model_date is 20230315 and the model_hour is 00

    '''
    model_date, model_hour = re.search(r'(\d{8}).t(\d{2})', key).groups()
    return model_date, model_hour


def parse_nos_model_run_datestamp_offset(key: str) -> Tuple[str, int]:
    '''
    Parse the model run forecast time key from the key of the file in the NOS S3 bucket, given the NOS naming convention: 
        'nos/dbofs/nos.dbofs.fields.f001.20230315.t00z.nc.zarr' 
    where the model_date is 20230315 and the model_hour is 00 and the offset is 1, this would result in a key of 20230315T01
    '''
    offset, model_date, model_hour = re.search(r'[f|n](\d{3}).(\d{8}).t(\d{2})', key).groups()
    model_date = datetime.datetime.strptime(f'{model_date}T{model_hour}', '%Y%m%dT%H') + datetime.timedelta(hours=int(offset))
    model_date_key = model_date.strftime('%Y%m%dT%H')
    return model_date_key, int(offset)


def generate_nos_model_run_glob_expression(key: str, model_date: str, model_hour: str) -> Tuple[str, ModelRunType]: 
    '''
    Parse the glob prefix and postfix given the zarr single file key: 
        'nos/dbofs/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    The following expression will be created: nos/dbofs/nos.dbofs.fields.f*.{model_date}.t{model_hour}z.nc.zarr'
    '''
    prefix, run_type, postfix = re.search(r'(.*).([f|n])\d{3}.\d{8}.t\d{2}z.(.*)', key).groups()
    model_run_type = ModelRunType.from_offset_prefix(run_type)
    glob_expression = '[0-9][0-9][0-9]'
    return f'{prefix}.{run_type}{glob_expression}.{model_date}.t{model_hour}z.{postfix}', model_run_type


def generate_nos_best_time_series_glob_expression(key: str) -> str:
    '''
    Parse the glob prefix and postfix given the zarr single file key: 
        'nos/dbofs/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    The following expression will be created: nos/nos.dbofs.fields.f*.*.t*z.nc.zarr'
    '''
    prefix, postfix = re.search(r'(.*).f\d{3}.\d{8}.t\d{2}z.(.*)', key).groups()
    glob_expression = 'f[0-9][0-9][0-9]'
    return f'{prefix}.{glob_expression}.*.t*z.{postfix}'


def generate_kerchunked_nos_model_run(bucket: str, key: str, concat_dims=List[str], identical_dims=List[str]):
    '''
    Generate or update the multizarr kerchunked aggregation for the model run that the specified file belongs to
    '''
    try:
        model_date, model_hour = parse_nos_model_run_datestamp(key)
        model_run_glob, model_run_type = generate_nos_model_run_glob_expression(key, model_date, model_hour)
    except Exception as e:
        print(f'Failed to parse model run date and hour from key {key}: {e}. Skipping...')
        return

    # For now SSL false is solving my cert issues **shrug**
    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True)

    model_run_files = fs_read.glob(f's3://{bucket}/{model_run_glob}')
    model_run_files = sorted(['s3://'+f for f in model_run_files])

    model_run_file_count = len(model_run_files)
    print(f'Aggregating {model_run_file_count} model files for model run {model_date} t{model_hour}z...')

    mzz = MultiZarrToZarr(
        model_run_files, 
        remote_protocol='s3', 
        remote_options={'anon': True},
        concat_dims=concat_dims,
        identical_dims=identical_dims
    )

    d = mzz.translate()

    model_run_type_name = model_run_type.name.lower()
    outkey = model_run_glob.replace('f*', model_run_type_name).replace('n*', model_run_type_name)

    outurl = f's3://{bucket}/{outkey}'

    print(f'Writing zarr model aggregation to {outurl}')
    with fs_write.open(outurl, 'w') as ofile:
        ofile.write(ujson.dumps(d))
    
    print(f'Successfully updated {outurl} NOS aggregation')


def generate_kerchunked_nos_best_time_series(bucket: str, key: str, concat_dims=List[str], identical_dims=List[str]):
    '''
    Generate or update the best time series kerchunked aggregation for the model run. If the specified file is not in the best time series, 
    then the best time series aggregation will not be updated
    '''
    print(f'Generating best time series multizarr aggregation for key: {key}')

    try:
        best_time_series_glob = generate_nos_best_time_series_glob_expression(key)
    except Exception as e: 
        print(f'Failed to parse model run date and hour from key {key}: {e}. Skipping...')
        return

    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True)

    model_files = fs_read.glob(f's3://{bucket}/{best_time_series_glob}')
    model_files = sorted(['s3://'+f for f in model_files])

    indexes = {}

    for f in model_files:
        model_date_key, offset = parse_nos_model_run_datestamp_offset(f)
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

    # TODO: Generalize this somehow? 
    mzz = MultiZarrToZarr(
        model_best_files, 
        remote_protocol='s3', 
        remote_options={'anon': True},
        concat_dims=concat_dims,
        identical_dims=identical_dims
    )

    d = mzz.translate()

    outkey = best_time_series_glob.replace('f*', 'best').replace('.*.t*z', '')
    outurl = f's3://{bucket}/{outkey}'

    print(f'Writing zarr best time series aggregation to {outurl}')
    with fs_write.open(outurl, 'w') as ofile:
        ofile.write(ujson.dumps(d))
    
    print(f'Successfully updated {outurl} NOS best time series aggregation')