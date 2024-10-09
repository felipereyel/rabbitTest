import pika
import time
import random

import pika.adapters.blocking_connection


def work(ch: pika.adapters.blocking_connection.BlockingChannel, method, _, body):
    print(f"INIT {body}")
    time.sleep(3 * random.random())

    print(f"DONE {body}")
    ch.basic_ack(delivery_tag=method.delivery_tag)
