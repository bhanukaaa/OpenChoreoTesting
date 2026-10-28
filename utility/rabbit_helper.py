import pika
import os
import time
import json


def get_connection():
    host = os.getenv('RABBITMQ_HOST') or os.getenv('RABBIT_HOST', 'rabbitmq')
    port = int(os.getenv('RABBITMQ_PORT', 5672))
    user = os.getenv('RABBITMQ_USER', 'guest')
    password = os.getenv('RABBITMQ_PASSWORD', 'guest')
    credentials = pika.PlainCredentials(user, password)

    for attempt in range(10):
        try:
            print(
                f"[*] Attempting to connect to RabbitMQ ({host}:{port}, Attempt {attempt + 1}/10)...", flush=True)
            return pika.BlockingConnection(pika.ConnectionParameters(
                host=host,
                port=port,
                credentials=credentials
            ))
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            print(f"[!] RabbitMQ not ready yet ({e}). Retrying in 5 seconds...", flush=True)
            time.sleep(5)

    raise Exception(f"Could not connect to RabbitMQ at {host}:{port} after 10 attempts.")


def publish_event(channel, exchange, routing_key, payload):
    try:
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )
        print(
            f"[*] Published persistent event '{routing_key}' to '{exchange}'", flush=True)
    except Exception as e:
        print(f"[!] Error publishing event {routing_key}: {e}", flush=True)
        raise e


def declare_exchange(channel, exchange_name, exchange_type='topic', durable=True):
    try:
        channel.exchange_declare(
            exchange=exchange_name, exchange_type=exchange_type, durable=durable)
        print(
            f"[*] Declared exchange '{exchange_name}' (type: {exchange_type})", flush=True)
    except Exception as e:
        print(f"[!] Error declaring exchange {exchange_name}: {e}", flush=True)
        raise e


def declare_queue(channel, queue_name, durable=True, exclusive=False, dead_letter_exchange=None, dead_letter_routing_key=None):
    try:
        arguments = {}
        if dead_letter_exchange:
            arguments['x-dead-letter-exchange'] = dead_letter_exchange
        if dead_letter_routing_key:
            arguments['x-dead-letter-routing-key'] = dead_letter_routing_key

        result = channel.queue_declare(queue=queue_name, durable=durable,
                                       exclusive=exclusive, arguments=arguments if arguments else None)
        actual_queue_name = result.method.queue
        print(
            f"[*] Declared queue '{actual_queue_name}' (durable: {durable}, exclusive: {exclusive})", flush=True)
        return actual_queue_name
    except Exception as e:
        print(f"[!] Error declaring queue {queue_name}: {e}", flush=True)
        raise e


def bind_queue(channel, queue_name, exchange_name, routing_key):
    try:
        channel.queue_bind(exchange=exchange_name,
                           queue=queue_name, routing_key=routing_key)
        print(
            f"[*] Bound queue '{queue_name}' to exchange '{exchange_name}' with routing key '{routing_key}'", flush=True)
    except Exception as e:
        print(f"[!] Error binding queue {queue_name}: {e}", flush=True)
        raise e


def setup_listener(channel, queue_name, callback, auto_ack=True):
    try:
        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)
        print(f"[*] Started consuming from queue '{queue_name}'", flush=True)
        channel.start_consuming()
    except Exception as e:
        print(f"[!] Error setting up listener: {e}", flush=True)
        raise e


def listen_to_events(channel, exchange_name, queue_config, callback):
    try:
        declare_exchange(channel, exchange_name)

        if queue_config.get('dead_letter_exchange'):
            declare_exchange(
                channel, queue_config['dead_letter_exchange'], exchange_type='direct')
            dlq_name = queue_config.get(
                'dead_letter_queue_name', queue_config['queue_name'] + '_dlq')
            declare_queue(channel, dlq_name, durable=True)
            bind_queue(channel, dlq_name, queue_config['dead_letter_exchange'], queue_config.get(
                'dead_letter_routing_key', 'dlq'))

        queue_name = declare_queue(
            channel,
            queue_config['queue_name'],
            durable=queue_config.get('durable', True),
            exclusive=queue_config.get('exclusive', False),
            dead_letter_exchange=queue_config.get('dead_letter_exchange'),
            dead_letter_routing_key=queue_config.get('dead_letter_routing_key')
        )

        for routing_key in queue_config['routing_keys']:
            bind_queue(channel, queue_name, exchange_name, routing_key)

        setup_listener(channel, queue_name, callback,
                       auto_ack=queue_config.get('auto_ack', True))
    except Exception as e:
        print(f"[!] Error in listen_to_events: {e}", flush=True)
        raise e
