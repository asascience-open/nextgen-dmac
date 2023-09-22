import re
import datetime
from typing import Tuple

import fsspec
import ujson
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr


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
    offset, model_date, model_hour = re.search(r'f(\d{3}).(\d{8}).t(\d{2})', key).groups()
    model_date = datetime.datetime.strptime(f'{model_date}T{model_hour}', '%Y%m%dT%H') + datetime.timedelta(hours=int(offset))
    model_date_key = model_date.strftime('%Y%m%dT%H')
    return model_date_key, int(offset)


def generate_nos_model_run_glob_expression(key: str, model_date: str, model_hour: str) -> str: 
    '''
    Parse the glob prefix and postfix given the zarr single file key: 
        'nos/dbofs/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    The following expression will be created: nos/dbofs/nos.dbofs.fields.f*.{model_date}.t{model_hour}z.nc.zarr'
    '''
    prefix, postfix = re.search(r'(.*).f\d{3}.\d{8}.t\d{2}z.(.*)', key).groups()
    return f'{prefix}.f*.{model_date}.t{model_hour}z.{postfix}'


def generate_nos_best_time_series_glob_expression(key: str) -> str:
    '''
    Parse the glob prefix and postfix given the zarr single file key: 
        'nos/dbofs/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    The following expression will be created: nos/nos.dbofs.fields.f*.*.t*z.nc.zarr'
    '''
    prefix, postfix = re.search(r'(.*).f\d{3}.\d{8}.t\d{2}z.(.*)', key).groups()
    return f'{prefix}.f*.*.t*z.{postfix}'


def generate_nos_output_key(key: str) -> str:
    '''
    Generate the output file key for a given input key and destination bucket and prefix:
        'tbofs.20230314/nos.tbofs.fields.n002.20230314.t00z.nc'
    The following output key will be generated: tbofs/nos.tbofs.fields.n002.20230314.t00z.nc.zarr'
    '''
    parts = key.split('/')
    model_name = parts[0].split('.')[0]
    return f'{model_name}/{parts[1]}.zarr'


def generate_kerchunked_nos_nc(region: str, bucket: str, key: str, dest_bucket: str, dest_prefix: str):
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
    filekey = generate_nos_output_key(key)
    outurl = f"s3://{dest_bucket}/{dest_prefix}/{filekey}"

    with fs_read.open(url) as ifile:
        print(f"Kerchunking nos model at {url}")
        try:
            chunks = SingleHdf5ToZarr(ifile, url)
        except Exception as e:
            print(f'Failed to kerchunk {url}: {e}')
            return

        print(f"Writing kerchunked nos model to {outurl}")
        with fs_write.open(outurl, mode="w") as ofile:
            data = ujson.dumps(chunks.translate())
            ofile.write(data)
    
    print(f'Successfully processed {url}')


def generate_kerchunked_nos_roms_model_run(region: str, bucket: str, key: str):
    '''
    Generate or update the multizarr kerchunked aggregation for the model run that the specified file belongs to
    '''
    try:
        model_date, model_hour = parse_nos_model_run_datestamp(key)
        model_run_glob = generate_nos_model_run_glob_expression(key, model_date, model_hour)
    except Exception as e:
        print(f'Failed to parse model run date and hour from key {key}: {e}. Skipping...')
        return

    # For now SSL false is solving my cert issues **shrug**
    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True, use_ssl=False)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True, use_ssl=False)

    model_run_files = fs_read.glob(f's3://{bucket}/{model_run_glob}')
    model_run_files = sorted(['s3://'+f for f in model_run_files])

    model_run_file_count = len(model_run_files)
    print(f'Aggregating {model_run_file_count} model files for model run {model_date} t{model_hour}z...')

    # TODO: Generalize this somehow? 
    mzz = MultiZarrToZarr(
        model_run_files, 
        remote_protocol='s3', 
        remote_options={'anon': True, 'use_ssl': False},
        concat_dims=['ocean_time'],
        identical_dims=['eta_rho', 'xi_rho', 's_rho', 'eta_psi', 'xi_psi', 's_w', 'eta_u', 'xi_u', 'eta_v', 'xi_v', 'lat_rho', 'lat_psi', 'lat_u', 'lat_v', 'lon_rho', 'lon_psi', 'lon_u', 'lon_v']
    )

    d = mzz.translate()

    outkey = model_run_glob.replace('.f*', '')
    outurl = f's3://{bucket}/{outkey}'

    print(f'Writing zarr model aggregation to {outurl}')
    with fs_write.open(outurl, 'w') as ofile:
        ofile.write(ujson.dumps(d))
    
    print(f'Successfully updated {outurl} NOS aggregation')


def generate_kerchunked_nos_roms_best_time_series(region: str, bucket: str, key: str):
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
    
    # For now SSL false is solving my cert issues **shrug**
    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True, use_ssl=False)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True, use_ssl=False)

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
        remote_options={'anon': True, 'use_ssl': False},
        concat_dims=['ocean_time'],
        identical_dims=['eta_rho', 'xi_rho', 's_rho', 'eta_psi', 'xi_psi', 's_w', 'eta_u', 'xi_u', 'eta_v', 'xi_v', 'lat_rho', 'lat_psi', 'lat_u', 'lat_v', 'lon_rho', 'lon_psi', 'lon_u', 'lon_v']
    )

    d = mzz.translate()

    outkey = best_time_series_glob.replace('f*', 'best').replace('.*.t*z', '')
    outurl = f's3://{bucket}/{outkey}'

    print(f'Writing zarr best time series aggregation to {outurl}')
    with fs_write.open(outurl, 'w') as ofile:
        ofile.write(ujson.dumps(d))
    
    print(f'Successfully updated {outurl} NOS best time series aggregation')
