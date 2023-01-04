import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


API_KEY='RBEROQNVMBFK2AKN'
ENDPOINT_SCHEMA_URL='https://psrc-k0w8v.us-central1.gcp.confluent.cloud'
API_SECRET_KEY='1p4j+p5x4ybjxKjdC0WNCBPVvmDWnl6NPxB5YDppxiWyExGTzPV+tpk7MqCA51sL'
BOOTSTRAP_SERVER='pkc-3w22w.us-central1.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL='SASL_SSL'
SSL_MECHANISM='PLAIN'
SCHEMA_REGISTRY_API_KEY='Z3IQOJWULF27EHBN'
SCHEMA_REGISTRY_API_SECRET='6FcyvC7/GAj9bjhrR9wFI4sm0bYI1vvCDIIlRRwdo3dFUUS3Z5kf59cPR1qpcUWb'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MECHANISM,
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
      "item_name": {
      "description": "The string type is used.",
      "type": "string"
    },
    "order_date": {
      "description": "The string type is used.",
      "type": "string"
    },
    "order_number": {
      "description": "The integer type is used.",
      "type": "integer"
    },
    "product_price": {
      "description": "The number type is used.",
      "type": "number"
    },
    "quantity": {
      "description": "The integer type is used.",
      "type": "integer"
    },
    
    "total_products": {
      "description": "The integer type is used.",
      "type": "integer"
    }
   },
  "title": "SampleRecord",
  "type": "object"
}
    """
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if order is not None:
                print("User record {}: order: {}\n"
                      .format(msg.key(), order))
        except KeyboardInterrupt:
            break

    consumer.close()

main("topic_0")