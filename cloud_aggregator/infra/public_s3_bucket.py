import pulumi
from pulumi_aws import s3


class BucketExpirationRule:
    '''
    A lifecycle rule for an S3 bucket.
    '''
    def __init__(self, prefix: str, days: int):
        '''Defines a lifecycle rule for a public S3 bucket.
        
        :param prefix: The prefix to apply the rule to.
        :param days: The number of days after which to expire objects.
        '''
        self.prefix = prefix
        self.days = days


class PublicS3Bucket(pulumi.ComponentResource):
    '''
    A public S3 bucket with a lifecycle policy that expires objects after a certain number of days.
    '''

    def __init__(self, bucket_name: str, expiration_rules: list[BucketExpirationRule] = [], opts = None):
        '''
        Creates a public S3 bucket with a lifecycle policy that expires objects after a certain number of days.

        :param bucket_name: The name of the bucket to create.
        :param expiration_rules: A list of BucketExpirationRule objects that define the lifecycle policy.
        :param opts: Options to pass to the component.
        '''
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
            lifecycle_rules=lifecycle_rules,
            opts=pulumi.ResourceOptions(parent=self)
        )

        # Make the bucket public
        self.bucket_public_access_block = s3.BucketPublicAccessBlock(
            f'{bucket_name}-public-bucket-access-block',
            bucket=self.bucket.id,
            block_public_acls=False,
            block_public_policy=False,
            ignore_public_acls=False,
            restrict_public_buckets=False,
        )

        self.public_bucket_policy = s3.BucketPolicy(
            f'{bucket_name}-public-bucket-policy', 
            bucket=self.bucket.id,
            policy=self.bucket.arn.apply(lambda arn: {
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
                        f"{arn}/*",
                        f"{arn}"
                    ]
                }]
            }), 
            opts=pulumi.ResourceOptions(parent=self, depends_on=self.bucket)
        )
        
        self.register_outputs({
            'bucket_name': self.bucket.bucket,
            'bucket_policy_name': self.public_bucket_policy.id,
        })
