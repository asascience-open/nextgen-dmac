from ingest_tools.pipeline import PipelineContext
from ingest_tools.nos_ofs import NOS_Pipeline
from ingest_tools.rtofs import RTOFS_Pipeline
from ingest_tools.aws import parse_s3_sqs_payload


# TODO: Make these configurable
DESTINATION_BUCKET_NAME='nextgen-dmac-cloud-ingest'

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

    context = PipelineContext(region, DESTINATION_BUCKET_NAME)

    # TODO: These could get auto-registered
    context.add_pipeline('nos_ofs', NOS_Pipeline())
    context.add_pipeline('rtofs', RTOFS_Pipeline())

    matching = context.get_matching_pipelines(key)
    for pipeline in matching:
        pipeline.run(context.get_region(), bucket, key, context.get_dest_bucket())    