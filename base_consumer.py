from typing import Callable

import pika
from pika.adapters.blocking_connection import BlockingChannel

from configs import CONN_PARAMS, PREFETCH_COUNT, QUEUE


def run(consume: Callable[[pika.BlockingConnection, BlockingChannel, str], None]):
    connection = pika.BlockingConnection(CONN_PARAMS)
    with connection:
        channel = connection.channel()
        with channel:
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.basic_qos(prefetch_count=PREFETCH_COUNT)

            try:
                consume(connection, channel, QUEUE)
            except KeyboardInterrupt:
                requeued_messages = channel.cancel()
                print("Requeued %i messages" % requeued_messages)
