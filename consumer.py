from src.consumer.json_customer import consume_data
import os

if __name__ == '__main__':
    topic = 'sensor-topic'
    file_path = os.path.join(os.getcwd(), 'data', 'aps_failure_training_set1.csv')
    consume_data(topic=topic, file_path=file_path)