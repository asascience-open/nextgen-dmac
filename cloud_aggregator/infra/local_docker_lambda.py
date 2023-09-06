import pulumi
from pulumi_aws import iam, lambda_, s3, sqs
from pulumi_awsx import ecr


class LocalDockerLambda(pulumi.ComponentResource):
    '''
    A dockerized lambda function located in the local file tree
    '''

    def __init__(self, name: str, repo: str, path: str, timeout: int, memory_size: int, concurrency: int,  opts=None):
        '''
        Creates a dockerized lambda function from files located in the local file tree

        :param name: The name of the lambda function
        :param repo: The name of the ecr repository to push the image to
        :param path: The relative path to the lambda function in the local file tree
        :param timeout: The timeout for the lambda function in seconds
        :param memory_size: The memory size for the lambda function in MB
        :param concurrency: The number of concurrent executions allowed for the lambda function
        :param opts: Options to pass to the component
        '''
        super().__init__('infra:local_docker_lambda:LocalDockerLambda', name, None, opts)

        # An ECR repository to store our ingest images
        # TODO: Figure out how this works as a public image with ecrpublic
        self.repo = ecr.Repository(repo, opts=pulumi.ResourceOptions(parent=self))

        # Build and publish our ingest container image from ./ingest to the ECR repository.
        self.image = ecr.Image(
            f"{repo}-image",
            repository_url=self.repo.url.apply(lambda url: f"{url}"),
            path=path,
            opts=pulumi.ResourceOptions(parent=self, depends_on=self.repo)
        )

        self.lambda_role = iam.Role(
            f"{name}-lambda-role", 
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

        self.lambda_ = lambda_.Function(
            name, 
            package_type='Image', 
            image_uri=self.image.image_uri.apply(lambda uri: f"{uri}"), 
            role=self.lambda_role.arn.apply(lambda arn: f"{arn}"),
            timeout=timeout,
            reserved_concurrent_executions=concurrency,
            memory_size=memory_size,
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.image, self.lambda_role])
        )

        self.register_outputs({
            "lambda_arn": self.lambda_.arn,
            "lambda_name": self.lambda_.name,
        })

    def attach_policy_to_role(self, attachment_name: str, policy: iam.Policy):
        '''
        Attach a given policy to the lambda role

        :param attachment_name: The name of the policy attachment
        :param policy: The policy to attach to the lambda role

        :returns: The policy attachment
        '''
        return iam.RolePolicyAttachment(
            attachment_name,
            role=self.lambda_role.name.apply(lambda name: f"{name}"), 
            policy_arn=policy.arn.apply(lambda arn: f"{arn}"),
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.lambda_role, policy])
        )

    def add_cloudwatch_log_access(self):
        '''
        Gives the lambda function access to cloudwatch logs via the lambda role

        :returns: The policy attachment
        '''
        policy_name = f"{self._name}_cloudwatch_policy"
        cloudwatch_policy = iam.Policy(
            f"{self._name}_cloudwatch_policy",
            description='Policy to write data to cloudwatch logs',
            path='/', 
            policy={
                "Version": "2012-10-17",
                "Statement": [
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
            },
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.lambda_role])
        )

        return self.attach_policy_to_role(f"{policy_name}_attachment", cloudwatch_policy)

    def add_s3_access(self, policy_name: str, bucket: s3.bucket.Bucket):
        '''
        Gives the lambda function access to the given s3 bucket via the lambda role

        :param policy_name: The name of the policy to create
        :param bucket: The bucket to give the lambda function access to

        :returns: The policy attachment
        '''
        lambda_s3_policy = iam.Policy(
            policy_name, 
            description='Policy to write data to s3 bucket',
            path='/', 
            policy={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "s3:PutObject",
                        "Resource": bucket.arn.apply(lambda arn: f"{arn}/*"),
                        "Effect": "Allow"
                    },
                ]
            },
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.lambda_role, bucket])
        )

        return self.attach_policy_to_role(f"{policy_name}_{self._name}_attachment", lambda_s3_policy)

    def subscribe_to_sqs(self, subscription_name: str, queue: sqs.Queue, batch_size: int):
        '''
        Subscribes the lambda function to an sqs queue

        :param subscription_name: The name of the subscription to create
        :param sqs_queue: The sqs queue to subscribe to
        :param batch_size: The number of messages to retrieve from the queue per lambda invocation

        :returns: The event source mapping
        '''
        # First we have to give the lambda access to the sqs queue
        policy_name = f"{subscription_name}_policy"
        lambda_sqs_policy = iam.Policy(
            f"{subscription_name}_policy",
            description='Policy to access sqs queue',
            path='/', 
            policy={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "sqs:*",
                        "Resource": queue.arn.apply(lambda arn: f"{arn}"),
                        "Effect": "Allow"
                    },
                ]
            },
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.lambda_role, queue])
        )

        lambda_sqs_policy_attachemnt = self.attach_policy_to_role(f"{policy_name}_attachment", lambda_sqs_policy)

        # Then we can create the event mapping
        # TODO: Include filters! (https://www.pulumi.com/registry/packages/aws/api-docs/lambda/eventsourcemapping/#sqs-with-event-filter)
        # TODO: Use larger batches? (https://www.pulumi.com/registry/packages/aws/api-docs/lambda/eventsourcemapping/#batch-size)
        return lambda_.EventSourceMapping(
            subscription_name, 
            event_source_arn=queue.arn.apply(lambda arn: f"{arn}"),
            function_name=self.lambda_.arn.apply(lambda arn: f"{arn}"),
            batch_size=batch_size,
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.lambda_, lambda_sqs_policy_attachemnt, queue])
        )