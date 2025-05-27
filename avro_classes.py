# To avoid code repetition and to improve clarity,
# creating this file to contain classes and repeated functions 
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
import os

class cityTemp(object):
   # Arguments: cityName (string), cityTemperatureReading (float), readingDate (date)
   def __init__ (self, cityName_string=None, cityTemperature_double=None, readingDate_date=None):
      self.cityName_string = cityName_string
      self.cityTemperature_double = cityTemperature_double
      self.readingDate_date = readingDate_date

def cityTemp_to_dict(cityTemp, ctx):
   # this function takes in an object of the cityTemp class, and converts the values 
   # to a dict (dictionary)
   return dict(cityName_string = cityTemp.cityName_string,
                cityTemperature_double = cityTemp.cityTemperature_double,
                readingDate_date = cityTemp.readingDate_date)

def dict_to_cityTemp(obj, ctx):
   # this function takes in an object (avro object), and converts the values 
   # to the cityTemp class, ready for printing (original format)
    if obj is None:
        return None


    return cityTemp(cityName_string=obj['cityName_string'],
                    cityTemperature_double=obj['cityTemperature_double'],
                    readingDate_date=obj['readingDate_date'])

def get_schema(val):
  # creates a new schema registry client instance, and using the schema reg
  # endpoint and the name of the schema, imports the avro schema from confluent cloud
  schema_registry_client_instance = SchemaRegistryClient({
    'url': 'https://psrc-8qmnr.eu-west-2.aws.confluent.cloud',
    'basic.auth.user.info': 'FZMR46IXTWERIT7I:sUeRvvfHkHJaL9iKzswghBSQXPrbqMP7AIcH+qsP4YQ5NN/p43MMr1W4nOmsv7YR'
   })
  
  if val == 1:
    return schema_registry_client_instance
  elif val == 2:
     return schema_registry_client_instance.get_latest_version('faker_topic_test-value')
  elif val == 3:
     return schema_registry_client_instance.get_schema('faker_topic_test-value')
  elif val == 4:
     path = os.path.realpath(os.path.dirname(__file__))
     with open(f"{path}/schema.avsc") as f:
        schema_from_file = f.read()
     return schema_from_file
  else:
     print("Invalid Val")