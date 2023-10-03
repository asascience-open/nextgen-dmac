"""
Generic utility functions for working with AWS
"""
import ujson
from typing import Tuple


def parse_s3_sqs_payload(sqs_payload: str) -> Tuple[str, str, str]:
    """
    Parse the SQS message from the S3 bucket to extract the file metadata

    :param sqs_payload: The SQS payload from the S3 bucket
    :returns: A tuple of the region, bucket, and key
    """
    sqs_message = ujson.loads(sqs_payload)
    message = ujson.loads(sqs_message["Message"])
    record = message["Records"][0]
    region = record["awsRegion"]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    return region, bucket, key
