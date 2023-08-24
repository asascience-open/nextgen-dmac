import pulumi
from pulumi_aws import s3


class BucketExpirationRule:
    def __init__(self, prefix, days):
        self.prefix = prefix
        self.days = days


class PublicS3Bucket(pulumi.ComponentResource):
    def __init__(self, bucket_name, expiration_rules = [], opts = None):
        super().__init__('infra:public_s3_bucket:PublicS3Bucket', bucket_name, None, opts)

        lifecycle_rules = [
            s3.BucketLifecycleRuleArgs(
                    enabled=True,
                    expiration=s3.BucketLifecycleRuleExpirationArgs(
                        days=rule.days
                    ),
                    id='expiration',
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
            lifecycle_rules=lifecycle_rules
        )

        # Make the bucket public
        bucket_public_access_block = s3.BucketPublicAccessBlock(f"{bucket_name}-public",
            bucket=self.bucket.id,
            block_public_acls=False,
            block_public_policy=False,
            ignore_public_acls=False,
            restrict_public_buckets=False
        )

        public_bucket_policy = s3.BucketPolicy(
            f'{bucket_name}-public-policy', 
            bucket=self.bucket.id,
            policy={
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "s3:GetObject", 
                        "s3:GetObjectVersion", 
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        self.bucket.arn.apply(lambda arn: f"{arn}/*"),
                        self.bucket.arn.apply(lambda arn: f"{arn}")
                    ]
                }]
            }
        )
        
        self.register_outputs({
            'bucket_name': self.bucket.bucket,
        })