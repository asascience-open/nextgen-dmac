import ingest_tools.aws as aws


def test_sqs_payload_extraction():
    with open('tests/data/s3_sqs_payload.json', 'r') as f:
        sqs_payload = f.read()
    
    region, bucket, key = aws.parse_s3_sqs_payload(sqs_payload)
    assert region == 'us-east-1'
    assert bucket == 'noaa-ofs-pds'
    assert key == 'tbofs.20230314/nos.tbofs.fields.n002.20230314.t00z.nc'