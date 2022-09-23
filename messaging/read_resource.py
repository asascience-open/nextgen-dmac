#!/usr/bin/env python
import os
import sys
import pika

rabbitconnect = os.getenv('RABBIT_CONNECTION', 'localhost')
credentials = pika.PlainCredentials('app', 'app')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(rabbitconnect, 5672, '/', credentials))
channel = connection.channel()

channel.queue_declare(queue='resource_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


# reads resource names from the queue and assigns an object id
def read_resource(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())

    # TODO: Generate a unique ID to be used across the system
    # Insert unique ID into a Redis key/value store as objectId:resource -> Create example of querying
    # Send a new message so listeners who need the resource can start acting (KVP or Tuple if possible)
    # That queue will serve as a hub for file conversion, etc; things like Zarr conversion, Copy to S3, etc
    # Any failures should be redirected so that they can be notified and handled

    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='resource_queue', on_message_callback=read_resource)
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)