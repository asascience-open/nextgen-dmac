from ingest_tools.nos_ofs import ROMS_Agg_Pipeline, FVCOM_Agg_Pipeline, SELFE_Agg_Pipeline
from ingest_tools.rtofs import RTOFS_Agg_Pipeline
from ingest_tools.aws import parse_s3_sqs_payload


def handler(event, context):
    """
    This is the entry point for the aggregate lambda function. It is responsible for
    taking the s3 event from the ingest bucket object updated notification
    and processing it. This means scanning the given key path, in the given bucket,
    finding all relevant virtual dataset file, aggragating them together into
    a single virtual dataset, and writing that virtual dataset to the given bucket and
    key path.
    """
    payload = event["Records"][0]["body"]

    print(f"Updating aggregations from given notification: {payload}")

    region, bucket, key = parse_s3_sqs_payload(payload)

    pipelines = []
    pipelines.append(ROMS_Agg_Pipeline())
    pipelines.append(FVCOM_Agg_Pipeline())
    pipelines.append(SELFE_Agg_Pipeline())
    pipelines.append(RTOFS_Agg_Pipeline())

    ran=False
    for p in pipelines:        
        pipeline = p
        if pipeline.accepts(key):
            ran=True
            pipeline.generate_kerchunk(bucket, key)

    if not ran:
        print(f"No aggregation available for key: {key}")
