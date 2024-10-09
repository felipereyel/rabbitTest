import pika

from work import work
from configs import QUEUE, CONN_PARAMS, PREFETCH_COUNT


connection = pika.BlockingConnection(CONN_PARAMS)
with connection:
    channel = connection.channel()
    with channel:
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.basic_qos(prefetch_count=PREFETCH_COUNT)

        try:
            for method, properties, body in channel.consume(queue=QUEUE):
                work(channel, method, properties, body)
        except KeyboardInterrupt:
            requeued_messages = channel.cancel()
            print("Requeued %i messages" % requeued_messages)
