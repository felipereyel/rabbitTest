import pika

# https://pika.readthedocs.io/en/stable/examples/blocking_delivery_confirmations.html
CONN_PARAMS = pika.ConnectionParameters(host="localhost", heartbeat=60)

QUEUE = "test-queue"
EXCHANGE = ""

PREFETCH_COUNT = 5
