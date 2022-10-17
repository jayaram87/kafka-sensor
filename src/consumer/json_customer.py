from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from src.entity.generic import Generic
from src.constants.secrets import *
from src.dbops.mongodb import DBOps
import os


def kafka_cluster_config():
    config = {
        'sasl.mechanism': 'PLAIN',
        'bootstrap.servers': api_url,
        'security.protocol': 'SASL_SSL',
        'sasl.username': api_key,
        'sasl.password': api_secret
    }
    return config

def consume_data(topic, file_path):
    schema_str = Generic.get_topic_schema(path=file_path)
    json_deserializer = JSONDeserializer(schema_str, from_dict=Generic.dict_to_object)

    consumer_config = kafka_cluster_config()
    consumer_config.update({
        'group.id': 'group1',
        'auto.offset.reset': 'earliest'
    })

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    db = DBOps()
    data = []
    x = 0
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue

        record: Generic = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if record is not None:
            data.append(record.to_dict())
            if x % 500 == 0:
                db.insert_many(col_name='car', records=data)
                data = []
        x += 1

    consumer.close()

if __name__ == '__main__':
    consume_data('sensor-topic', os.path.join(os.getcwd(), 'data', 'aps_failure_training_set1.csv'))