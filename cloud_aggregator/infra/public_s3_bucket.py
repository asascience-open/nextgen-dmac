from typing import List, Optional
import pulumi
import pulumi_aws as aws
from pulumi_aws import iam, s3, sns


class BucketExpirationRule:
    """
    A lifecycle rule for an S3 bucket.
    """

    def __init__(self, prefix: str, days: int):
        """Defines a lifecycle rule for a public S3 bucket.

        :param prefix: The prefix to apply the rule to.
        :param days: The number of days after which to expire objects.
        """
        self.prefix = prefix
        self.days = days


class PublicS3Bucket(pulumi.ComponentResource):
    """
    A public S3 bucket with a lifecycle policy that expires objects after a certain number of days.
    """

    def __init__(
        self,
        bucket_name: str,
        expiration_rules: list[BucketExpirationRule] = [],
        opts=None,
    ):
        """
        Creates a public S3 bucket with a lifecycle policy that expires objects after a certain number of days.

        :param bucket_name: The name of the bucket to create.
        :param expiration_rules: A list of BucketExpirationRule objects that define the lifecycle policy.
        :param opts: Options to pass to the component.
        """
        super().__init__(
            "infra:public_s3_bucket:PublicS3Bucket", bucket_name, None, opts
        )

        lifecycle_rules = [
            s3.BucketLifecycleRuleArgs(
                enabled=True,
                expiration=s3.BucketLifecycleRuleExpirationArgs(days=rule.days),
                id="expiration",
                prefix=rule.prefix,
                noncurrent_version_expiration=s3.BucketLifecycleRuleNoncurrentVersionExpirationArgs(
                    days=7
                ),
            )
            for rule in expiration_rules
        ]

        self.bucket = s3.Bucket(
            bucket_name,
            bucket=bucket_name,
            lifecycle_rules=lifecycle_rules,
            opts=pulumi.ResourceOptions(parent=self),
        )

        # Make the bucket public
        self.bucket_public_access_block = s3.BucketPublicAccessBlock(
            f"{bucket_name}-public-bucket-access-block",
            bucket=self.bucket.id,
            block_public_acls=False,
            block_public_policy=False,
            ignore_public_acls=False,
            restrict_public_buckets=False,
        )

        self.public_bucket_policy = s3.BucketPolicy(
            f"{bucket_name}-public-bucket-policy",
            bucket=self.bucket.id,
            policy=self.bucket.arn.apply(
                lambda arn: {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": [
                                "s3:GetObject",
                                "s3:GetObjectVersion",
                                "s3:ListBucket",
                            ],
                            "Resource": [f"{arn}/*", f"{arn}"],
                        }
                    ],
                }
            ),
            opts=pulumi.ResourceOptions(parent=self, depends_on=self.bucket),
        )

        self.register_outputs(
            {
                "bucket_name": self.bucket.bucket,
                "bucket_policy_name": self.public_bucket_policy.id,
            }
        )

    # TODO: This can be refactored but it works for now
    @staticmethod
    def basic_subscribe_sns_to_bucket_notifications(
        bucket: aws.s3.GetBucketResult, subscription_name: str, sns_topic: sns.Topic, filter_prefix: List[str] = [], filter_suffix: Optional[str] = None):

        bucket_topic_policy_document = iam.get_policy_document_output(
            statements=[
                iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    principals=[
                        iam.GetPolicyDocumentStatementPrincipalArgs(
                            type="Service",
                            identifiers=["s3.amazonaws.com"],
                        )
                    ],
                    actions=["SNS:Publish"],
                    resources=[sns_topic.arn.apply(lambda arn: f"{arn}")],
                    conditions=[
                        iam.GetPolicyDocumentStatementConditionArgs(
                            test="ArnLike",
                            variable="aws:SourceArn",
                            values=[bucket.arn],
                        )
                    ],
                )
            ]
        )

        bucket_topic_policy = sns.TopicPolicy(
            f"{subscription_name}_policy",
            arn=sns_topic.arn.apply(lambda arn: f"{arn}"),
            policy=bucket_topic_policy_document.json,
            opts=pulumi.ResourceOptions(depends_on=[sns_topic]),
        )

        # Subscribe the topic to the bucket
        # TODO: Make events configurable
        bucket_notification = s3.BucketNotification(
            subscription_name,
            bucket=bucket.id,
            topics=[
                s3.BucketNotificationTopicArgs(
                    topic_arn=sns_topic.arn.apply(lambda arn: f"{arn}"),
                    events=[
                        "s3:ObjectCreated:*",
                        "s3:ObjectRemoved:*",
                    ],
                    filter_prefix=prefix,
                    filter_suffix=filter_suffix,
                ) for prefix in filter_prefix
            ],
            opts=pulumi.ResourceOptions(
                depends_on=[bucket_topic_policy]
            ),
        )

        return bucket_notification       
     

    def subscribe_sns_to_bucket_notifications(
        self, subscription_name: str, sns_topic: sns.Topic, filter_prefix: List[str] = [], filter_suffix: Optional[str] = None
    ):
        """
        Subscribes an SNS topic to bucket object notifications.

        :param subscription_name: The name of the subscription to create.
        :param sns_arn: The ARN of the SNS topic to subscribe to.
        :param filter_suffix: The suffix to filter notifications by.
        """
        bucket_topic_policy_document = iam.get_policy_document_output(
            statements=[
                iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    principals=[
                        iam.GetPolicyDocumentStatementPrincipalArgs(
                            type="Service",
                            identifiers=["s3.amazonaws.com"],
                        )
                    ],
                    actions=["SNS:Publish"],
                    resources=[sns_topic.arn.apply(lambda arn: f"{arn}")],
                    conditions=[
                        iam.GetPolicyDocumentStatementConditionArgs(
                            test="ArnLike",
                            variable="aws:SourceArn",
                            values=[self.bucket.arn.apply(lambda arn: f"{arn}")],
                        )
                    ],
                )
            ]
        )

        bucket_topic_policy = sns.TopicPolicy(
            f"{subscription_name}_policy",
            arn=sns_topic.arn.apply(lambda arn: f"{arn}"),
            policy=bucket_topic_policy_document.json,
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.bucket, sns_topic]),
        )

        # Subscribe the topic to the bucket
        # TODO: Make events configurable
        bucket_notification = s3.BucketNotification(
            subscription_name,
            bucket=self.bucket.id.apply(lambda id: f"{id}"),
            topics=[
                s3.BucketNotificationTopicArgs(
                    topic_arn=sns_topic.arn.apply(lambda arn: f"{arn}"),
                    events=[
                        "s3:ObjectCreated:*",
                        "s3:ObjectRemoved:*",
                    ],
                    filter_prefix=prefix,
                    filter_suffix=filter_suffix,
                ) for prefix in filter_prefix
            ],
            opts=pulumi.ResourceOptions(
                parent=self, depends_on=[self.bucket, bucket_topic_policy]
            ),
        )

        return bucket_notification
