from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from src.entity.generic import Generic, instance_to_dict
from src.constants.secrets import *
from src.logger import logging
import os

def schema_config():
    return {'url': schema_url, 'basic.auth.user.info': f'{schema_api_key}:{schema_api_secret}'}

def kafka_cluster_config():
    config = {
        'sasl.mechanism': 'PLAIN',
        'bootstrap.servers': api_url,
        'security.protocol': 'SASL_SSL',
        'sasl.username': api_key,
        'sasl.password': api_secret
    }
    return config

def delivery_report(err, msg):
    if err is not None:
        logging.info(f'Delivery failed {msg.key()} with error {err.value()}')
    else:
        logging.info(f'Record {msg.key} produced to {msg.topic()} at partition {msg.partition()} with offset {msg.offset()}')



def producer_data(topic, file_path):
    schema_str = Generic.get_topic_schema(path=file_path)
    schema_registry_config = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, instance_to_dict)

    producer = Producer(kafka_cluster_config())

    producer.poll(0.0) # start producing at poll 0

    for instance in Generic.get_object(path=file_path):
        print(instance)
        producer.produce(topic=topic, 
            key=string_serializer(str(uuid4()), instance.to_dict()),
            value=json_serializer(instance, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=delivery_report)

    producer.flush()

if __name__ == '__main__':
    producer_data('sensor-topic', os.path.join(os.getcwd(), 'data', 'aps_failure_training_set1.csv'))