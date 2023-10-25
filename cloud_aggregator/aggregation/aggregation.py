from ingest_tools.nos_ofs import (
    generate_kerchunked_nos_roms_model_run,
    generate_kerchunked_nos_roms_best_time_series,
    generate_kerchunked_nos_fvcom_model_run,
    generate_kerchunked_nos_fvcom_best_time_series,
    generate_kerchunked_nos_selfe_model_run,
    generate_kerchunked_nos_selfe_best_time_series,
)
from ingest_tools.rtofs import generate_kerchunked_rtofs_best_time_series
from ingest_tools.aws import parse_s3_sqs_payload
from ingest_tools.filters import key_contains


NOS_ROMS_FILTERS = ["cbofs", "ciofs", "dbofs", 'gomofs', "tbofs", "wcofs"]
NOS_FVCOM_FILTERS = ["leofs", "lmhofs", "loofs", 'lsofs', "ngofs2", "sfbofs"]
NOS_SELFE_FILTERS = ['creofs']
RTOFS_FILTERS = ["rtofs"]


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

    if key_contains(key, NOS_ROMS_FILTERS):
        generate_kerchunked_nos_roms_model_run(region, bucket, key)
        generate_kerchunked_nos_roms_best_time_series(region, bucket, key)
    elif key_contains(key, NOS_FVCOM_FILTERS):
        generate_kerchunked_nos_fvcom_model_run(region, bucket, key)
        generate_kerchunked_nos_fvcom_best_time_series(region, bucket, key)
    elif key_contains(key, NOS_SELFE_FILTERS):
        generate_kerchunked_nos_selfe_model_run(region, bucket, key)
        generate_kerchunked_nos_selfe_best_time_series(region, bucket, key)
    elif key_contains(key, RTOFS_FILTERS):
        generate_kerchunked_rtofs_best_time_series(region, bucket, key)
    else:
        print(f"No aggregation available for key: {key}")
