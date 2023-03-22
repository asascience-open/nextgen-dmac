import fsspec
import ujson
from kerchunk.hdf import SingleHdf5ToZarr


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


def parse_nos_sqs_message(sqs_payload: str):
    '''
    Parse the SQS message from the NOS ROMS S3 bucket to extract the file metadata needed for kerchunking and uploading 
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


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 4:
        if len(sys.argv) > 2 and sys.argv[1] == "--dump":
            print(sys.argv[2])
            sys.exit(0)
        else:
            print("Usage: python kerchunk_nos_roms.py <sqs_message> <dest_bucket> <dest_prefix> <key_filter>")
            sys.exit(1)

    sqs_payload = sys.argv[1]
    dest_bucket = sys.argv[2]
    dest_prefix = sys.argv[3]
    key_filter = sys.argv[4] if len(sys.argv) > 4 else None

    generate_kerchunked_nos_sqs(sqs_payload, dest_bucket, dest_prefix, key_filter)
