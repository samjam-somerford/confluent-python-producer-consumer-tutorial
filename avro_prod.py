from confluent_kafka import Producer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from faker import Faker
from avro_classes import *

import socket
import random
import time

# this code sets the configurations for connecting to our kafka cluster
# in confluent cloud - just change these values when connecting to a new 
# cluster - these are the barebones of what we need

conf = {'bootstrap.servers': '*** ENTER YOUR BOOTSTRAP URL HERE ***',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '*** ENTER YOUR API KEY HERE ***',
        'sasl.password': '*** ENTER YOUR API KEY SECRET HERE ***',
        'client.id': socket.gethostname()}

def acked(err, msg):
  if err is not None:
    print("Failed to deliver messages: %s: %s" % (str(msg), str(err)))
  else:
    print("Message produced successfully")

def produce(serializer, topic, config):
  # creates a new producer and faker instance
  producer = Producer(config)
  topic_name = topic
  faker = Faker()
  string_serializer = StringSerializer("utf-8")
  # produces a sample message using faker and random for fake data

  tempObj = cityTemp(faker.city(), round(random.uniform(0.0, 30.0),2), faker.date())
  cityTemp_gen_dict = cityTemp_to_dict(tempObj, SerializationContext(topic_name, MessageField.VALUE))
  
  # gets city name from the obj we'll use as our key for each record
  city_dict_list_values = list(cityTemp_gen_dict.values())
  city_key = city_dict_list_values[0]

  # creates 
  value_ser = serializer(tempObj, SerializationContext(topic_name, MessageField.VALUE))

  print("Producing: ",city_dict_list_values," to topic: ", topic_name)
  producer.produce(topic=topic_name, 
                   key=string_serializer(str(city_key)), 
                   value=value_ser,
                   callback=acked)
  
  # print(f"Produced message to topic {topic}: key = {city_key} value = {cityTemp_gen_dict}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()


def basic_producer_loop(serializer, topic, config):
    # loop for producing to a topic with the mock data in the produce function
    # loop for producing every 5 seconds
    while True:
        produce(serializer, topic, config)
        time.sleep(1)


def main(config):
  topic = "*** ENTER YOUR TOPIC NAME HERE ***"
  schema_client = get_schema(1)
  schema_str = get_schema(4)
  
  avro_serializer = AvroSerializer(schema_client,
                                    schema_str,
                                    cityTemp_to_dict)
  
  basic_producer_loop(avro_serializer, topic, config)
  

main(conf)
