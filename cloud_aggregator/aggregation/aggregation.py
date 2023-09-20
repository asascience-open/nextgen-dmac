from ingest_tools.nos_ofs import parse_nos_sqs_message, generate_kerchunked_roms_model_run, generate_kerchunked_roms_best_time_series


ROMS_FILTERS = ['cbofs', 'ciofs', 'dbofs', 'tbofs', 'wcofs']


def handler(event, context):
    '''
    This is the entry point for the aggregate lambda function. It is responsible for
    taking the s3 event from the ingest bucket object updated notification 
    and processing it. This means scanning the given key path, in the given bucket, 
    finding all relevant virtual dataset file, aggragating them together into 
    a single virtual dataset, and writing that virtual dataset to the given bucket and
    key path.
    '''
    payload = event['Records'][0]['body']

    print(f'Updating aggregations from given notification: {payload}')

    region, bucket, key = parse_nos_sqs_message(payload)
    for k in ROMS_FILTERS:
        if k in key:
            generate_kerchunked_roms_model_run(region, bucket, key)
            generate_kerchunked_roms_best_time_series(region, bucket, key)
            return

    print(f'No aggregation available for key: {key}')
