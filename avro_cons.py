from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField
from avro_classes import *

import sys
import socket
import random
import time

schema_client = get_schema(1)
schema_str = get_schema(4)


avro_deserializer = AvroDeserializer(schema_client,
                                     schema_str,
                                     dict_to_cityTemp)

string_deserializer = StringDeserializer('utf_8')

group_id = random.randint(0, 1000)

conf = {'bootstrap.servers': '*** ENTER YOUR BOOTSTRAP URL HERE ***',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '*** ENTER YOUR API KEY ***',
        'sasl.password': '*** ENTER YOUR API KEY SECRET ***',
        'group.id': group_id,
        'enable.auto.commit': 'true',
        'auto.offset.reset': 'earliest',
        'client.id': socket.gethostname()}

running = True
consumerInst = Consumer(conf)
cities_consumed = []

def print_assignment(consumer,partitions):
    print('Assignment:',partitions)

def consume_loop(deserializer, consumer, topics):
    try:
        consumer.subscribe(topics)
        while running:
            print("Consuming.")
            msgcount = 0
            string_deserializer = StringDeserializer('utf_8')
            msg = consumer.poll(1.0)
            if msg is None: 
                print("MESSAGE: NONE")
                continue
            if msg.error():
                raise KafkaException(msg.error())
                continue
            
            byte_message = msg.value()
            cityTemp_msg = deserializer(byte_message, SerializationContext(msg.topic(), MessageField.VALUE))
            if cityTemp is not None:
                printVal = ("CityTemp Record Offset: {} \n City: {}, "
                          "City Temperature: {}, "
                          "Temperature Reading Date: {}\n")
                print(printVal.format(msg.offset(), cityTemp_msg.cityName_string, 
                                      cityTemp_msg.cityTemperature_double,
                                      cityTemp_msg.readingDate_date))
           
                time.sleep(2)
    
    except KeyboardInterrupt:
        # stop consuming on user key press
        sys.stderr.write('%% Aborted by user\n')
    
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()



topic_to_consume = ["*** ENTER YOUR TOPIC NAME HERE ***"]
consume_loop(avro_deserializer, consumerInst, topic_to_consume)
