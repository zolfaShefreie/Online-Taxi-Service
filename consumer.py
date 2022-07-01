import json
from kafka import KafkaConsumer

if __name__ == '__main__':
    consumer = KafkaConsumer('data', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
    for data in consumer:
        print(json.loads(data.value))
