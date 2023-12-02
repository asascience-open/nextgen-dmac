"""Serverless Kerchunk infrastructure using Pulumi and AWS"""

import pulumi
from pulumi import log
from pulumi_aws import sns
import pulumi_aws as aws

from infra.public_s3_bucket import PublicS3Bucket, BucketExpirationRule
from infra.local_docker_lambda import LocalDockerLambda
from infra.message_queue import MessageQueue


config = pulumi.Config()
pipelines = config.require_object('pipelines')
buckets = config.require_object('buckets')

# Create the queue for the aggregation lambda
aggregation_queue = MessageQueue(
    'aggregation-queue',
    visibility_timeout=720,
)

# create bucket resources from the buckets section of the config
bucket_resources = {}
bucket_dependencies = []

for bucket_name in buckets:
    bucket_conf = buckets.get(bucket_name)
    existing_arn = bucket_conf.get('existing_arn')
    bucket = None
    if existing_arn:
        #bucket = aws.get_arn(arn=existing_arn)
        bucket = aws.s3.get_bucket(bucket=bucket_name)
        #bucket = aws.s3.Bucket.get(bucket_name)
        if not bucket:
            log.error(f'{bucket_name} ARN not found: {existing_arn}')
    else:
        log.info(f'Creating {bucket_name}')

        # Create the bucket to ingest data into
        bucket = PublicS3Bucket(
            bucket_name=bucket_name,
            expiration_rules=[
                BucketExpirationRule(
                    prefix='nos/',
                    days=29,
                )
            ]
        )
        bucket_dependencies.append(bucket)
    if bucket:
        bucket_resources[bucket_name] = bucket

# for each pipeline, create aggregation notifications for the destination bucket
# so when new files are added, it can check if a new aggregation needs to be created
for p in pipelines:
    pline = pipelines.get(p)
    dest_bucket = pline.get('dest_bucket')    
        
    # Any bucket mentioned in the config should be subscribed to for notifications
    if not dest_bucket in bucket_resources:
        raise ValueError(f'{dest_bucket} does not exist in the buckets configuration or the resource unable to be created')
    bucket = bucket_resources[dest_bucket]

    # First create an SNS topic for the bucket notifications
    ingest_bucket_notifications_topic = sns.Topic(
        f'{dest_bucket}-object-notification'
    )

    # Subscribe the bucket object notifications to the SNS topic
    # for the aggregation queue

    if isinstance(bucket, PublicS3Bucket):
        bucket.subscribe_sns_to_bucket_notifications(
            subscription_name=f'{dest_bucket}-notifications-subscription',
            sns_topic=ingest_bucket_notifications_topic,
            filter_prefix=pline.get('filter_prefix'),
            filter_suffix=pline.get('filter_suffix')
        )    
    else:
        PublicS3Bucket.basic_subscribe_sns_to_bucket_notifications(
                bucket,
                subscription_name=f'{dest_bucket}-notifications-subscription',
                sns_topic=ingest_bucket_notifications_topic,
                filter_prefix=pline.get('filter_prefix'),
                filter_suffix=pline.get('filter_suffix')
        )    

    # Subscribe the aggregation queue to the ingestion bucket SNS topic.
    # Note that this creates a many to one relationship:
    # i.e. many notifications will go through the same aggregation queue.
    aggregation_queue.subscribe_to_sns(
        subscription_name=f'{dest_bucket}-updated-subscription',
        sns_arn=ingest_bucket_notifications_topic.arn.apply(lambda arn: f"{arn}"))

# Export the name of the bucket
# TODO: Needed?
#pulumi.export('bucket_name', bucket.bucket.id)

# Next we need the sns topic of the NODD service that we want to subscribe to
# TODO: This should be a config value.
nodd_nos_topic_arn = 'arn:aws:sns:us-east-1:123901341784:NewOFSObject'
nodd_rtofs_topic_arn = 'arn:aws:sns:us-east-1:709902155096:NewRTOFSObject'

# First nos ofs queue
new_ofs_object_queue = MessageQueue(
    'nos-new-ofs-object-queue',
    visibility_timeout=360,
)

new_ofs_object_subscription = new_ofs_object_queue.subscribe_to_sns(
    subscription_name='nos-new-ofs-object-subscription',
    sns_arn=nodd_nos_topic_arn,
)

# next, rtofs queue
new_rtofs_object_queue = MessageQueue(
    'new-rtofs-object-queue',
    visibility_timeout=360,
)

new_rtofs_object_subscription = new_rtofs_object_queue.subscribe_to_sns(
    subscription_name='new-rtofs-object-subscription',
    sns_arn=nodd_rtofs_topic_arn,
)

# Create the lambda to ingest NODD data into the bucket
# TODO: Decrease memory
ingest_lambda = LocalDockerLambda(
    name="ingest-nos-to-zarr",
    repo="nextgen-dmac-ingest",
    path='./ingest',
    timeout=60,
    memory_size=1024,
    concurrency=6,
)

# Add cloudwatch, s3, and sqs access to the lambda. Finally subscribe the lambda 
# to the new object queue
ingest_lambda.add_cloudwatch_log_access()
ingest_lambda.add_s3_access('ingest-s3-lambda-policy', bucket_dependencies)

# Subscribe to the necessary queues
ingest_lambda.subscribe_to_sqs(
    subscription_name='nos-sqs-lambda-mapping',
    queue=new_ofs_object_queue.queue,
    batch_size=1,
)

ingest_lambda.subscribe_to_sqs(
    subscription_name='rtofs-sqs-lambda-mapping',
    queue=new_rtofs_object_queue.queue,
    batch_size=1,
)

# Okay now for the aggregation. This part of the infra will create an sqs queue that receives bucket notifications
# from the ingest bucket. The queue will then trigger a lambda function that will aggregate the data into a single
# zarr store. The zarr store will then be uploaded to the ingestion bucket.

# TODO: Decrease memory
aggregation_lambda = LocalDockerLambda(
    name="aggregate-nos-zarr", 
    repo="nextgen-dmac-aggregation",
    path='./aggregation',
    timeout=240,
    memory_size=1536,
    concurrency=6,
)

# Add cloudwatch, s3, and sqs access to the lambda. Finally subscribe the lambda 
# to the aggregation queue
aggregation_lambda.add_cloudwatch_log_access()
aggregation_lambda.add_s3_access('aggregation-s3-lambda-policy', bucket_dependencies)
aggregation_lambda.subscribe_to_sqs(
    subscription_name='nos-aggregation-lambda-mapping',
    queue=aggregation_queue.queue,
    batch_size=1,
)
