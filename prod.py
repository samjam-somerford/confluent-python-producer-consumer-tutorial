from confluent_kafka import Producer

import socket
from faker import Faker
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
        'client.id': socket.gethostname()}

def acked(err, msg):
  if err is not None:
    print("Failed to deliver messages: %s: %s" % (str(msg), str(err)))
  else:
    print("Message produced successfully")

def produce(topic, config):
  # creates a new producer and faker instance
  producer = Producer(config)
  faker = Faker()

  # generate then encode values
  gen_value = {'city': faker.city(), 'temperature': random.uniform(0.0, 30.0)}
  gen_value_str_encoded = bytes(str(gen_value), 'utf-8')
  key = "example_key"
  
  producer.produce(topic, key=key, value=gen_value_str_encoded, callback=acked)
  print(f"Produced message to topic {topic}: key = {key} value = {gen_value_str_encoded}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()


def basic_producer_loop(topic, config):
    # loop for producing to a topic with the mock data in the produce function
    # loop for producing every 5 seconds
    while True:
        produce(topic, config)
        time.sleep(5)

# simple way to call the produce loop with a given topic name
topic_to_produce_to = "*** ENTER YOUR TOPIC NAME HERE ***"
basic_producer_loop(topic_to_produce_to, conf)
