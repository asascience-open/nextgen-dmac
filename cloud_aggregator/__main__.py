"""Serverless Kerchunk infrastructure using Pulumi and AWS"""

import json
import pulumi
from pulumi_aws import s3, sns, sqs, iam, lambda_
from pulumi_awsx import ecr

from infra.public_s3_bucket import PublicS3Bucket, BucketExpirationRule
from infra.message_queue import MessageQueue


# Create the bucket to ingest data into
bucket = PublicS3Bucket(
    bucket_name='nextgen-dmac-cloud-ingest',
    expiration_rules=[
        BucketExpirationRule(
            prefix='nos/',
            days=29,
        )
    ]
)

# Export the name of the bucket
pulumi.export('bucket_name', bucket.bucket.id)

# Next we need the sns topic of the NODD service that we want to subscribe to
# TODO: This should be a config value.
nodd_nos_topic_arn = 'arn:aws:sns:us-east-1:123901341784:NewOFSObject'

new_ofs_object_queue = MessageQueue(
    'nos-new-ofs-object-queue',
    visibility_timeout=360,
)

new_ofs_object_subscription = new_ofs_object_queue.subscribe_to_sns(
    subscription_name='nos-new-ofs-object-subscription',
    sns_arn=nodd_nos_topic_arn,
)

# # We need a dead letter queue to handle messages that fail to process 
# # for the nos new object topic
# new_ofs_object_dlq = sqs.Queue(
#     'nos-new-ofs-object-dlq',
#     sqs_managed_sse_enabled=False,
# )

# # We create an sqs queue to ingest the messages from the NOS new object topic
# # Default the visibility timeout to 10 minutes to try and handle surges of data dumps
# new_ofs_object_queue = sqs.Queue(
#     'nos-new-ofs-object-queue', 
#     sqs_managed_sse_enabled=False,
#     visibility_timeout_seconds=360,
#     redrive_policy=new_ofs_object_dlq.arn.apply(lambda arn: json.dumps({
#         "deadLetterTargetArn": arn,
#         "maxReceiveCount": 4
#     }))
# )

# # Give the SNS message from the NOS NODD bucket access to post to the sqs queue
# new_ofs_object_queue_policy_document = new_ofs_object_queue.arn.apply(lambda arn: iam.get_policy_document_output(statements=[iam.GetPolicyDocumentStatementArgs(
#     sid="First",
#     effect="Allow",
#     principals=[iam.GetPolicyDocumentStatementPrincipalArgs(
#         type="*",
#         identifiers=["*"],
#     )],
#     actions=["sqs:SendMessage"],
#     resources=[arn],
#     conditions=[iam.GetPolicyDocumentStatementConditionArgs(
#         test="ArnEquals",
#         variable="aws:SourceArn",
#         values=[nodd_nos_topic_arn]
#     )]
# )]))

# new_ofs_object_queue_policy = sqs.QueuePolicy(
#     "nos-new-ofs-object-queue-policy", 
#     queue_url=new_ofs_object_queue.id,
#     policy=new_ofs_object_queue_policy_document.json
# )

# # We create a subscription to the NOS new object topic
# new_ofs_subscription = sns.TopicSubscription(
#     'nos-new-ofs-object-subscription', 
#     topic=nodd_nos_topic_arn,
#     protocol='sqs',
#     endpoint=new_ofs_object_queue.arn
# )

# An ECR repository to store our ingest images
# TODO: Figure out how this works as a public image with ecrpublic
ingest_repo = ecr.Repository('nextgen-dmac-ingest')

# Build and publish our ingest container image from ./ingest to the ECR repository.
ingest_image = ecr.Image(
    'nextgen-dmac-ingest-image',
    repository_url=ingest_repo.url,
    path='./ingest',
)

# Assume the lambda role
ingest_lambda_role = iam.Role(
    'ingest-lambda-role', 
    assume_role_policy={
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Effect": "Allow"
        }]
    }
)

# Create the s3 policy for the given role
ingest_lambda_s3_policy = iam.Policy(
    'ingest-s3-lambda-policy', 
    description='Policy to write ingested data to s3',
    path='/', 
    policy={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "s3:PutObject",
                "Resource": bucket.bucket.arn.apply(lambda arn: f"{arn}/*"),
                "Effect": "Allow"
            },
            {
                "Action": "sqs:*",
                "Resource": new_ofs_object_queue.queue.arn.apply(lambda arn: f"{arn}"),
                "Effect": "Allow"
            }, 
            {
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*",
                "Effect": "Allow"
            }
        ]
    }
)

# Attach the lambda policy
iam.RolePolicyAttachment(
    "ingest-lambda-access",
    role=ingest_lambda_role.name, 
    policy_arn=ingest_lambda_s3_policy.arn,
)

# Create the lambda function to ingest nos netcdf to zarr virtual jsons
ingest_lambda = lambda_.Function(
    'ingest-nos-to-zarr', 
    package_type='Image', 
    image_uri=ingest_image.image_uri, 
    role=ingest_lambda_role.arn,
    timeout=60,
    reserved_concurrent_executions=6,
    memory_size=1024,
)

# Subscribe the new lambda to the NOS sqs queue
# TODO: Include filters! (https://www.pulumi.com/registry/packages/aws/api-docs/lambda/eventsourcemapping/#sqs-with-event-filter)
# TODO: Use larger batches? (https://www.pulumi.com/registry/packages/aws/api-docs/lambda/eventsourcemapping/#batch-size)
ingest_event_mapping = lambda_.EventSourceMapping(
    'nos-sqs-lambda-mapping', 
    event_source_arn=new_ofs_object_queue.queue.arn.apply(lambda arn: f"{arn}"),
    function_name=ingest_lambda.arn,
    batch_size=1,
)

# Okay now for the aggregation. This part of the infra will create an sqs queue that receives bucket notifications
# from the ingest bucket. The queue will then trigger a lambda function that will aggregate the data into a single
# zarr store. The zarr store will then be uploaded to the ingestion bucket.

# First create an SNS topic for the bucket notifications
ingest_bucket_notifications_topic = sns.Topic(
    'ingest-bucket-notifications-topic',
    name='ingest-object-updated'
)

ingest_bucket_topic_policy_document = iam.get_policy_document_output(statements=[iam.GetPolicyDocumentStatementArgs(
    effect="Allow",
    principals=[iam.GetPolicyDocumentStatementPrincipalArgs(
        type="Service",
        identifiers=["s3.amazonaws.com"],
    )],
    actions=["SNS:Publish"],
    resources=[ingest_bucket_notifications_topic.arn],
    conditions=[iam.GetPolicyDocumentStatementConditionArgs(
        test="ArnLike",
        variable="aws:SourceArn",
        values=[bucket.bucket.arn],
    )],
)])

sns.TopicPolicy(
    'ingest-bucket-notifications-topic-policy',
    arn=ingest_bucket_notifications_topic.arn,
    policy=ingest_bucket_topic_policy_document.json,
)

# Subscribe the topic to the bucket
s3.BucketNotification(
    'ingest-bucket-subscription',
    bucket=bucket.bucket.id,
    topics=[s3.BucketNotificationTopicArgs(
        topic_arn=ingest_bucket_notifications_topic.arn,
        events=[
            's3:ObjectCreated:*', 
            's3:ObjectRemoved:*',
        ],
        filter_suffix='.zarr',
    )],
)

# Create the queue for the aggregation lambda
aggregation_queue = sqs.Queue(
    'aggregation-queue',
    sqs_managed_sse_enabled=False,
    visibility_timeout_seconds=720,
)

# Give the bucket permission to post to the aggregation queue
aggregation_queue_policy_document = iam.get_policy_document_output(statements=[iam.GetPolicyDocumentStatementArgs(
    effect="Allow",
    principals=[iam.GetPolicyDocumentStatementPrincipalArgs(
        type="*",
        identifiers=["*"],
    )],
    actions=["sqs:SendMessage"],
    resources=[aggregation_queue.arn],
    conditions=[iam.GetPolicyDocumentStatementConditionArgs(
        test="ArnEquals",
        variable="aws:SourceArn",
        values=[ingest_bucket_notifications_topic.arn],
    )],
)])

sqs.QueuePolicy(
    'aggregation-queue-policy',
    queue_url=aggregation_queue.id,
    policy=aggregation_queue_policy_document.json,
)

# Subscribe the aggregation queue to the topic
aggregation_subscription = sns.TopicSubscription(
    'ingest-bucket-updated-subscription', 
    topic=ingest_bucket_notifications_topic.arn,
    protocol='sqs',
    endpoint=aggregation_queue.arn
)

# Create the role for the aggregation lambda
# An ECR repository to store our ingest images
# TODO: Figure out how this works as a public image with ecrpublic
aggregation_repo = ecr.Repository('nextgen-dmac-aggregation')

# Build and publish our ingest container image from ./ingest to the ECR repository.
aggregation_image = ecr.Image(
    'nextgen-dmac-aggregation-image',
    repository_url=aggregation_repo.url,
    path='./aggregation',
)

# Assume the lambda role
aggregation_lambda_role = iam.Role(
    'aggregation-lambda-role', 
    assume_role_policy={
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Effect": "Allow"
        }]
    }
)

# Create the s3 policy for the given role
aggregation_lambda_s3_policy = iam.Policy(
    'aggregation-s3-lambda-policy', 
    description='Policy to write aggregation data to s3',
    path='/', 
    policy={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "s3:PutObject",
                "Resource": bucket.bucket.arn.apply(lambda arn: f"{arn}/*"),
                "Effect": "Allow"
            },
            {
                "Action": "sqs:*",
                "Resource": aggregation_queue.arn.apply(lambda arn: f"{arn}"),
                "Effect": "Allow"
            }, 
            {
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*",
                "Effect": "Allow"
            }
        ]
    }
)

# Attach the lambda policy
iam.RolePolicyAttachment(
    "aggregation-lambda-access",
    role=aggregation_lambda_role.name, 
    policy_arn=aggregation_lambda_s3_policy.arn,
)

# Create the lambda function to aggregate the zarr stores
aggregation_lambda = lambda_.Function(
    'aggregate-nos-zarr', 
    package_type='Image', 
    image_uri=aggregation_image.image_uri, 
    role=aggregation_lambda_role.arn,
    timeout=120,
    memory_size=1536,
)

# Subscribe the new lambda to the aggregation sqs queue
# TODO: Include filters! (https://www.pulumi.com/registry/packages/aws/api-docs/lambda/eventsourcemapping/#sqs-with-event-filter)
# TODO: Use larger batches? (https://www.pulumi.com/registry/packages/aws/api-docs/lambda/eventsourcemapping/#batch-size)
ingest_event_mapping = lambda_.EventSourceMapping(
    'nos--aggregation-lambda-mapping', 
    event_source_arn=aggregation_queue.arn,
    function_name=aggregation_lambda.arn,
    batch_size=1,
)
