import pulumi
from pulumi_aws import sqs


class MessageQueue(pulumi.ComponentResource):
    '''
    An SQS queue with a dead letter queue.
    '''
    def __init__(self, queue_name: str, visibility_timeout: int = 360, opts = None):
        '''
        Creates an SQS queue with a dead letter queue.

        :param queue_name: The name of the queue to create.
        :param visibility_timeout: The number of seconds that messages will be invisible to consumers after being consumed.
        :param opts: Options to pass to the component.
        '''
        super().__init__('infra:message_queue:MessageQueue', queue_name, None, opts)

        self.dlq = sqs.Queue(
                f'{queue_name}-dlq',
                sqs_managed_sse_enabled=False,
                opts=pulumi.ResourceOptions(parent=self))

        self.queue = sqs.Queue(
            queue_name,
            sqs_managed_sse_enabled=False,
            visibility_timeout_seconds=visibility_timeout,
            redrive_policy=self.dlq.arn.apply(lambda arn: {
                "deadLetterTargetArn": arn,
                "maxReceiveCount": 4,
            }),
            opts=pulumi.ResourceOptions(parent=self)
        )

        self.register_outputs({
            'sqs_name': self.queue.name,
            'dlq_name': self.dlq.name,
        })
