import json

import pulumi
from pulumi_aws import sns, sqs, iam


class MessageQueue(pulumi.ComponentResource):
    """
    An SQS queue with a dead letter queue.
    """

    def __init__(self, queue_name: str, visibility_timeout: int = 360, opts=None):
        """
        Creates an SQS queue with a dead letter queue.

        :param queue_name: The name of the queue to create.
        :param visibility_timeout: The number of seconds that messages will be invisible to consumers after being consumed.
        :param opts: Options to pass to the component.
        """
        super().__init__("infra:message_queue:MessageQueue", queue_name, None, opts)

        self.dlq = sqs.Queue(
            f"{queue_name}-dlq",
            sqs_managed_sse_enabled=False,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.queue = sqs.Queue(
            queue_name,
            sqs_managed_sse_enabled=False,
            visibility_timeout_seconds=visibility_timeout,
            redrive_policy=self.dlq.arn.apply(
                lambda arn: json.dumps({
                    "deadLetterTargetArn": arn,
                    "maxReceiveCount": 4,
                })
            ),
            opts=pulumi.ResourceOptions(parent=self, depends_on=self.dlq),
        )

        self.register_outputs(
            {
                "sqs_name": self.queue.name,
                "dlq_name": self.dlq.name,
            }
        )

    def subscribe_to_sns(self, subscription_name: str, sns_arn: str, ):
        """
        Subscribes the SQS queue to an SNS topic.

        :param subscription_name: The name of the subscription to create.
        :param sns_arn: The ARN of the SNS topic to subscribe to.

        :return: The SNS Topic subscription.
        """
        policy_document = self.queue.arn.apply(
            lambda arn: iam.get_policy_document_output(
                statements=[
                    iam.GetPolicyDocumentStatementArgs(
                        sid="First",
                        effect="Allow",
                        principals=[
                            iam.GetPolicyDocumentStatementPrincipalArgs(
                                type="*",
                                identifiers=["*"],
                            )
                        ],
                        actions=["sqs:SendMessage"],
                        resources=[arn],
                        conditions=[
                            iam.GetPolicyDocumentStatementConditionArgs(
                                test="ArnEquals",
                                variable="aws:SourceArn",
                                values=[sns_arn],
                            )
                        ],
                    )
                ]
            )
        )

        _queue_policy = sqs.QueuePolicy(
            f"{subscription_name}_policy",
            queue_url=self.queue.id.apply(lambda id: f"{id}"),
            policy=policy_document.json,
            opts=pulumi.ResourceOptions(parent=self, depends_on=self.queue),
        )

        # We create a subscription to the NOS new object topic
        sns_subscription = sns.TopicSubscription(
            subscription_name,
            topic=sns_arn,
            protocol="sqs",
            endpoint=self.queue.arn.apply(lambda arn: f"{arn}"),
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.queue, _queue_policy]),
        )

        return sns_subscription
