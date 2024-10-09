import pika
from work import work
from configs import QUEUE, CONN_PARAMS, PREFETCH_COUNT


connection = pika.BlockingConnection(CONN_PARAMS)
with connection:
    channel = connection.channel()
    with channel:
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        channel.basic_consume(queue=QUEUE, on_message_callback=work)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
            requeued_messages = channel.cancel()
            print("Requeued %i messages" % requeued_messages)
