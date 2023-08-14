from ingest_tools.nos_ofs import generate_kerchunked_nos_multizarr_sqs


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

    generate_kerchunked_nos_multizarr_sqs(payload)