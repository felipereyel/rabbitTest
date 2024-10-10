import logging
import os
import random
import threading
import time

from pika.adapters.blocking_connection import BlockingChannel

LOG_FORMAT = "%(levelname) -10s %(asctime)s: %(message)s"
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def log(msg):
    logging.info(f"[{os.getpid()}] [{threading.get_ident()}] {msg}")


def work(body, method_frame):
    log(f"INIT [{method_frame.delivery_tag}] {body}")
    time.sleep(5 * random.random())
    log(f"DONE [{method_frame.delivery_tag}] {body}")


def ack(channel: BlockingChannel, method_frame):
    if channel.is_open:
        log(f"ACK  [{method_frame.delivery_tag}]")
        channel.basic_ack(method_frame.delivery_tag)
    else:
        log(
            f"ERRO [{method_frame.delivery_tag}]: Channel is closed, cannot ACK message"
        )
