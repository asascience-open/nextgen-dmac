from ingest_tools.nos_ofs import generate_kerchunked_nos_nc
from ingest_tools.aws import parse_s3_sqs_payload
from ingest_tools.filters import key_contains


# TODO: Make these configurable
DESTINATION_BUCKET_NAME='nextgen-dmac-cloud-ingest'
DESTINATION_PREFIX='nos'
ROMS_FILTERS=['cbofs', 'ciofs', 'dbofs', 'tbofs', 'wcofs']


def handler(event, context):
    '''
    This is the entry point for the ingest lambda function. It is responsible for
    taking the event from the NOS new object topic and processing it. This means
    scanning the netcdf file and extracting the metadata to create a virtual 
    zarr representation of the dataset in the referenced object. 
    '''
    payload = event['Records'][0]['body']
    
    print(f'Ingesting SQS Message: {payload}')

    region, bucket, key = parse_s3_sqs_payload(payload)

    if key_contains(key, ROMS_FILTERS):
        generate_kerchunked_nos_nc(region, bucket, key, DESTINATION_BUCKET_NAME, DESTINATION_PREFIX)
    else:
        print(f'No ingest available for key: {key}')