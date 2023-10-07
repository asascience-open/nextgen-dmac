from pipeline import PipelineContext
from pipeline import Pipeline
from ingest_tools.nos_ofs import generate_nos_outpu1t_key
from ingest_tools.rtofs import generate_kerchunked_rtofs_nc
from ingest_tools.aws import parse_s3_sqs_payload
from ingest_tools.filters import key_contains
from ingest_tools.generic import generate_kerchunked_hdf, generate_kerchunked_netcdf


# TODO: Make these configurable
DESTINATION_BUCKET_NAME='nextgen-dmac-cloud-ingest'
NOS_DESTINATION_PREFIX='nos'
RTOFS_DESTINATION_PREFIX='rtofs'
NOS_ROMS_FILTERS= ['cbofs', 'ciofs', 'dbofs', 'tbofs', 'wcofs']
NOS_FVCOM_FILTERS = ['ngofs2']
RTOFS_FILTERS = ['rtofs']

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

    context = PipelineContext(DESTINATION_BUCKET_NAME, NOS_DESTINATION_PREFIX)

    context.add_pipeline('nos_roms', Pipeline('.nc', NOS_ROMS_FILTERS))
    context.add_pipeline('fvcom', Pipeline('.nc', NOS_FVCOM_FILTERS))
    context.add_pipeline('rtofs', Pipeline('.nc', RTOFS_FILTERS))
    context.run(bucket, key)

    if key_contains(key, NOS_ROMS_FILTERS):
        output_key = generate_nos_output_key(key)
        generate_kerchunked_hdf(bucket, key, output_key, DESTINATION_BUCKET_NAME, NOS_DESTINATION_PREFIX)
    elif key_contains(key, NOS_FVCOM_FILTERS):
        output_key = generate_nos_output_key(key)
        generate_kerchunked_netcdf(bucket, key, output_key, DESTINATION_BUCKET_NAME, NOS_DESTINATION_PREFIX)
    elif key_contains(key, RTOFS_FILTERS):
        generate_kerchunked_rtofs_nc(region, bucket, key, DESTINATION_BUCKET_NAME, RTOFS_DESTINATION_PREFIX)
    else:
        print(f'No ingest available for key: {key}')