from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

from base_consumer import run
from work import work, ack


def for_consumer(_: BlockingConnection, channel: BlockingChannel, queue: str):
    for method_frame, header_frame, body in channel.consume(queue=queue):
        work(body)
        ack(channel, method_frame)


if __name__ == "__main__":
    run(for_consumer)
