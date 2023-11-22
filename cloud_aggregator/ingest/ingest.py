from ingest_tools.pipelineconfig import ConfigContext
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

    cc = ConfigContext()
    nos_config = cc.get_config('nos_kerchunk')
    nos_config.dest_bucket = 'get-from-pulumi'  
    rtofs_config = cc.get_config('rtofs_kerchunk')
    rtofs_config.dest_bucket = 'get-from-pulumi'

    context = PipelineContext()

    # TODO: These could get auto-registered
    context.add_pipeline('nos_ofs', NOS_Pipeline(nos_config))
    context.add_pipeline('rtofs', RTOFS_Pipeline(rtofs_config))

    matching = context.get_matching_pipelines(key)    
    for pipeline in matching:
        pipeline.run(bucket, key)    