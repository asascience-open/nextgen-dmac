from ingest_tools.nos_ofs import generate_kerchunked_nos_sqs


# TODO: Make these configurable
DESTINATION_BUCKET_NAME='nextgen-dmac-cloud-ingest'
DESTINATION_PREFIX='nos'
FILTERS=['cbofs', 'ciofs', 'dbofs', 'tbofs', 'wcofs']


def handler(event, context):
    '''
    This is the entry point for the ingest lambda function. It is responsible for
    taking the event from the NOS new object topic and processing it. This means
    scanning the netcdf file and extracting the metadata to create a virtual 
    zarr representation of the dataset in the referenced object. 
    '''
    payload = event['Records'][0]['body']
    
    print(f'Ingesting SQS Message: {payload}')

    generate_kerchunked_nos_sqs(payload, DESTINATION_BUCKET_NAME, DESTINATION_PREFIX, FILTERS)