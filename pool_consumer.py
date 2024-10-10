import concurrent.futures
import functools

from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

from base_consumer import run
from work import ack, work
from configs import PREFETCH_COUNT


def done_callback(connection, channel, method_frame, result):
    connection.add_callback_threadsafe(lambda: ack(channel, method_frame))


def pool_consumer(connection: BlockingConnection, channel: BlockingChannel, queue: str):
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=PREFETCH_COUNT, max_tasks_per_child=5
    ) as executor:
        for method_frame, header_frame, body in channel.consume(queue=queue):
            cb = functools.partial(done_callback, connection, channel, method_frame)
            result = executor.submit(work, body, method_frame)
            result.add_done_callback(cb)


if __name__ == "__main__":
    run(pool_consumer)
