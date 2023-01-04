import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd
from typing import List

FILE_PATH=r'C:\Users\rohir\OneDrive\Desktop\kafka\restaraunt_data\restaurant-take-away-data.csv'
columns=['order_number','order_date','item_name','quantity',
         'product_price','total_products']

API_KEY='RBEROQNVMBFK2AKN'
ENDPOINT_SCHEMA_URL='https://psrc-k0w8v.us-central1.gcp.confluent.cloud'
API_SECRET_KEY='1p4j+p5x4ybjxKjdC0WNCBPVvmDWnl6NPxB5YDppxiWyExGTzPV+tpk7MqCA51sL'
BOOTSTRAP_SERVER='pkc-3w22w.us-central1.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL='SASL_SSL'
SSL_MECHANISM='PLAIN'
SCHEMA_REGISTRY_API_KEY='Z3IQOJWULF27EHBN'
SCHEMA_REGISTRY_API_SECRET='6FcyvC7/GAj9bjhrR9wFI4sm0bYI1vvCDIIlRRwdo3dFUUS3Z5kf59cPR1qpcUWb'

def sasl_conf():
    
    sasl_conf = {'sasl.mechanism':SSL_MECHANISM,
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
         
def get_order_instance(file_path):
    df=pd.read_csv(file_path)
    #df=df.iloc[:,1:]
    orders:List[Order]=[]
    for data in df.values:
        order=Order(dict(zip(columns,data)))
        orders.append(order)
        yield order
        
def order_to_dict(order:Order,ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    
    return order.record
    
def delivery_report(err,msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """
    if err is not None:  
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully producted to {} [{}] at offset {}'.format(
        msg.key(),msg.topic(),msg.partition(),msg.offset()))
        
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
      "description": "The number type is used.",
      "type": "number"
    },
    "product_price": {
      "description": "The number type is used.",
      "type": "number"
    },
    "quantity": {
      "description": "The number type is used.",
      "type": "number"
    },
    
    "total_products": {
      "description": "The number type is used.",
      "type": "number"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    schema_registry_conf=schema_config()
    schema_registry_client=SchemaRegistryClient(schema_registry_conf)

    string_serializer =StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str,schema_registry_client,order_to_dict)
    
    producer = Producer(sasl_conf())
    
    print("Producing user records to topic {}. ^C to exit.".format(topic))

    producer.poll(0.0)
    try:
        for order in get_order_instance(file_path=FILE_PATH):

            print(order)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), order_to_dict),
                            value=json_serializer(order, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("topic_0")
     