from ingest_tools.pipelineconfig import ConfigContext
from ingest_tools.pipeline import PipelineContext
from ingest_tools.nos_ofs import NOS_Pipeline
from ingest_tools.rtofs import RTOFS_Pipeline
from ingest_tools.aws import parse_s3_sqs_payload
import pulumi


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

    config = pulumi.Config()

    region, bucket, key = parse_s3_sqs_payload(payload)

    cc = ConfigContext()
    nos_config = cc.get_config('nos_kerchunk')
    context = PipelineContext()

    # Configuration has to be pulumi-driven to not cross paths between dev/test/prod
    # Pulumi can update the configuration using Pulumi config when infra is deployed
    # configuration will be in the following format for maximum flexibility:
    # "pipelines" : {
    #    "nos_ofs": {
    #        "bucket": "name-of-bucket",
    #        "additional": "additional-metadata"
    #    }
    # }
    buckets = config.require('pipelines')
    if 'nos_ofs' in buckets:
        nos_config.dest_bucket = buckets.nos_ofs.bucket  
        context.add_pipeline('nos_ofs', NOS_Pipeline(nos_config))
    else:
        print('Missing config value pipelines.nos_ofs.bucket')
    
    rtofs_config = cc.get_config('rtofs_kerchunk')
    if 'rtofs' in buckets:
        rtofs_config.dest_bucket = buckets.rtofs.bucket
        context.add_pipeline('rtofs', RTOFS_Pipeline(rtofs_config))
    else:
        print('Missing config value pipelines.rtofs.bucket')

    matching = context.get_matching_pipelines(key)    
    for pipeline in matching:
        pipeline.run(bucket, key)    