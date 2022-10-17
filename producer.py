from src.producer.json_producer import producer_data
import os

if __name__ == '__main___':
    topic = 'sensor-topic'
    file_path = os.path.join(os.getcwd(), 'data', 'aps_failure_training_set1.csv')
    producer_data(topic=topic, file_path=file_path)