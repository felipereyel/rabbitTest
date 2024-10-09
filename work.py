import os
import random
import threading
import time

from pika.adapters.blocking_connection import BlockingChannel


def log(msg):
    print(f"[{os.getpid()}] [{threading.get_ident()}] {msg}")


def work(
    ch: BlockingChannel,
    method_frame,
    header_frame,
    body,
):
    log(f"INIT {body}")
    time.sleep(3 * random.random())

    log(f"DONE {body}")
    ch.basic_ack(delivery_tag=method_frame.delivery_tag)
