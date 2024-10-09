import sys
import pika
import uuid

from configs import QUEUE, EXCHANGE, CONN_PARAMS

n = int(sys.argv[1])
if not n:
    n = 100

connection = pika.BlockingConnection(CONN_PARAMS)
with connection:
    channel = connection.channel()
    with channel:
        channel.queue_declare(queue=QUEUE, durable=True)

        for i in range(n):
            body = f"{i + 1000} - {uuid.uuid4().hex[:8]}"
            channel.basic_publish(
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="text/plain",
                ),
                routing_key=QUEUE,
                exchange=EXCHANGE,
                body=body,
            )
