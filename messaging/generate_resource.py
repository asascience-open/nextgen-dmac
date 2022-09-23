#!/usr/bin/env python
import os
import pika
import sys

rabbitconnect = os.getenv('RABBIT_CONNECTION', 'localhost')
credentials = pika.PlainCredentials('app', 'app')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(rabbitconnect, 5672, '/', credentials))
channel = connection.channel()

channel.queue_declare(queue='resource_queue', durable=True)

# TODO: Send a message with actual resource to be read
# Send a json message
message = ' '.join(sys.argv[2:]) or "Hello World!"
channel.basic_publish(
    exchange='',
    routing_key='resource_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
    ))
print(" [x] Sent %r" % message)
connection.close()