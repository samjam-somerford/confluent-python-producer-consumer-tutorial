from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

import socket
import random
import time

# this code sets the configurations for connecting to our kafka cluster
# in confluent cloud - just change these values when connecting to a new 
# cluster

conf = {'bootstrap.servers': '*** ENTER YOUR BOOTSTRAP URL HERE ***',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '*** ENTER YOUR API KEY HERE ***',
        'sasl.password': '*** ENTER YOUR API KEY SECRET HERE ***',
        'group.id': 'foo',
        'enable.auto.commit': 'true',
        'auto.offset.reset': 'earliest',
        'client.id': socket.gethostname()}

running = True
consumerInst = Consumer(conf)

def consume_loop(consumer, topics):
    try:
        while running:
            consumer.subscribe(topics) 
            msgcount = 0
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' % 
                                 (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
                print(msg.value())

                time.sleep(2)
                # Store the offset associated with msg to a local cache.
                # Stored offsets are committed to Kafka by a background thread every 'auto.commit.interval.ms'.
                # Explicitly storing offsets after processing gives at-least once semantics.
                # consumer.store_offsets(msg)
    
    except KeyboardInterrupt:
        # stop consuming on user key press
        sys.stderr.write('%% Aborted by user\n')
    
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

topic_to_consume = ["*** ENTER YOUR TOPIC NAME HERE ***"]
consume_loop(consumerInst, topic_to_consume)
