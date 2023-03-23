import re
from typing import Tuple

import fsspec
import ujson
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr


def parse_model_run_datestamp(key: str) -> Tuple[str, str]:
    '''
    Parse the model run date from the key of the file in the NOS S3 bucket, given the NOS naming convention: 
        'nos/nos.dbofs.fields.f001.20230315.t00z.nc.zarr' 
    where the model_date is 20230315 and the model_hour is 00

    '''
    model_date, model_hour = re.search(r'(\d{8}).t(\d{2})', key).groups()
    return model_date, model_hour


def generate_model_run_glob_expression(key: str, model_date: str, model_hour: str) -> str: 
    '''
    Parse the glob prefix and postfix given the zarr single file key: 
        'nos/nos.dbofs.fields.f001.20230315.t00z.nc.zarr'
    The following expression will be created: nos/nos.dbofs.fields.f*.{model_date}.t{model_hour}z.nc.zarr'
    '''
    prefix, postfix = re.search(r'(.*).f\d{3}.\d{8}.t\d{2}z.(.*)', key).groups()
    return f'{prefix}.f*.{model_date}.t{model_hour}z.{postfix}'


def generate_kerchunked_nc(region: str, bucket: str, key: str, dest_bucket: str, dest_prefix: str):
    '''
    Generate a kerchunked zarr file from a netcdf file in s3
    '''
     # For now SSL false is solving my cert issues **shrug**
    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True, use_ssl=False)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True, use_ssl=False)

    url = f"s3://{bucket}/{key}"
    filekey = key.split("/")[-1]
    outurl = f"s3://{dest_bucket}/{dest_prefix}/{filekey}.zarr"

    with fs_read.open(url) as ifile:
        print(f"Kerchunking nos model at {url}")
        chunks = SingleHdf5ToZarr(ifile, url)

        print(f"Writing kerchunked nos model to {outurl}")
        with fs_write.open(outurl, mode="w") as ofile:
            data = ujson.dumps(chunks.translate())
            ofile.write(data)
    
    print(f'Successfully processed {url}')


def generate_kerchunked_model_run(region: str, bucket: str, key: str):
    '''
    Generate or update the multizarr kerchunked aggregation for the model run that the specified file belongs to
    '''
    # For now SSL false is solving my cert issues **shrug**
    fs_read = fsspec.filesystem('s3', anon=True, skip_instance_cache=True, use_ssl=False)
    fs_write = fsspec.filesystem('s3', anon=False, skip_instance_cache=True, use_ssl=False)

    model_date, model_hour = parse_model_run_datestamp(key)
    model_run_glob = generate_model_run_glob_expression(key, model_date, model_hour)
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

    print('Writing zarr model aggregation to {outurl}')
    with fs_write.open(outurl, 'w') as ofile:
        ofile.write(ujson.dumps(d).encode())
    
    print('Successfully updated {outurl} NOS aggregation')


def parse_nos_sqs_message(sqs_payload: str) -> Tuple[str, str, str]:
    '''
    Parse the SQS message from the S3 bucket to extract the file metadata needed for kerchunking and uploading 
    to a destination bucket
    '''
    sqs_message = ujson.loads(sqs_payload)
    message = ujson.loads(sqs_message["Message"])
    record = message["Records"][0]
    region = record["awsRegion"]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    return region, bucket, key


def generate_kerchunked_nos_sqs(sqs_payload: str, dest_bucket: str, dest_prefix: str, key_filter: str = None):
    '''
    Generate a kerchunked zarr file from a netcdf file in s3 given an SQS payload from the NOS S3 bucket
    '''
    region, bucket, key = parse_nos_sqs_message(sqs_payload)
    if key_filter is not None and key_filter not in key:
        print(f"Skipping {key} because it doesn't contain {key_filter}")
        return

    generate_kerchunked_nc(region, bucket, key, dest_bucket, dest_prefix)


def generate_kerchunked_nos_multizarr_sqs(sqs_payload: str):
    '''
    Given an SQS payload from the NOS S3 bucket, generate or update the multizarr kerchunked aggregation for the model run that the changed file belongs to
    '''
    region, bucket, key = parse_nos_sqs_message(sqs_payload)

    generate_kerchunked_model_run(region, bucket, key)


if __name__ == '__main__':
    import sys

    arg_count = len(sys.argv)

    if arg_count < 3:
        print("Usage: python kerchunk_nos_roms.py <command> <sqs_message> <dest_bucket> <dest_prefix> <key_filter>")
        sys.exit(1)
    
    command = sys.argv[1]
    sqs_payload = sys.argv[2]

    if command == 'dump': 
        print(sqs_payload)
        sys.exit(0)
    elif command == 'extract_single':
        dest_bucket = sys.argv[3]
        dest_prefix = sys.argv[4]
        key_filter = sys.argv[5] if len(sys.argv) > 5 else None
        generate_kerchunked_nos_sqs(sqs_payload, dest_bucket, dest_prefix, key_filter)
    elif command == 'update_model_run':
        generate_kerchunked_nos_multizarr_sqs(sqs_payload)
