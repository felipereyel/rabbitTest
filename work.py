import os
import random
import threading
import time

from pika.adapters.blocking_connection import BlockingChannel


def log(msg):
    print(f"[{os.getpid()}] [{threading.get_ident()}] {msg}")


def work(body):
    log(f"INIT {body}")
    time.sleep(3 * random.random())
    log(f"DONE {body}")


def ack(channel: BlockingChannel, method_frame):
    if channel.is_open:
        channel.basic_ack(method_frame.delivery_tag)
    else:
        log("Error: Channel is closed, cannot ACK message")
