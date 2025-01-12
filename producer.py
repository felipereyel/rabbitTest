import sys
import uuid

import pika

from configs import CONN_PARAMS, DEFAUT_EXCHANGE, QUEUE

n = int(sys.argv[1])
if not n:
    n = 100

connection = pika.BlockingConnection(CONN_PARAMS)
with connection:
    channel = connection.channel()
    with channel:
        channel.queue_declare(queue=QUEUE, durable=True)

        for i in range(n):
            body = f"{i + 1001} - {uuid.uuid4().hex[:8]}"
            channel.basic_publish(
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="text/plain",
                ),
                routing_key=QUEUE,
                exchange=DEFAUT_EXCHANGE,
                body=body,
            )
