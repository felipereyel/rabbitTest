from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

from base_consumer import run
from work import work, ack


def callback(
    channel: BlockingChannel,
    method_frame,
    header_frame,
    body,
):
    work(body)
    ack(channel, method_frame)


def cb_consumer(_: BlockingConnection, channel: BlockingChannel, queue: str):
    channel.basic_consume(queue=queue, on_message_callback=callback)
    channel.start_consuming()


if __name__ == "__main__":
    run(cb_consumer)
