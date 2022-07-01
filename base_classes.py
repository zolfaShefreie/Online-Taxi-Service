from kafka import KafkaProducer, KafkaConsumer, KafkaClient
import pandas as pd
import json


class BaseBlock:
    
    def __init__(self,  producer_topic: str, consumer_topic: str, bootstrap_servers: str, consumer_group_id: str, ):
        """
        initial function for each block.
        each block must produce sth and could be use another topic
        :param producer_topic: the name of topic that produce sth
        :param consumer_topic: the name of topic that is used in this black
        """
        self.producer_topic = producer_topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        self.consumer = KafkaConsumer(consumer_topic, bootstrap_servers=bootstrap_servers,
                                      auto_offset_reset='earliest',
                                      group_id=consumer_group_id) if consumer_topic else None

    def _produce_answer(self, entry_data):
        """
        this function do some process on entry_data and return answer
        :param entry_data:
        :return:
        """
        raise NotImplementedError

    def _send_data(self, data):
        """
        send data via producer to specific topic
        :param data:
        :return:
        """
        self.producer.send(self.producer_topic, data)

    def run(self):
        """
        the main management for block process
        get entry with consumer object (if it exists)
        do some process on entry_data
        send the result
        :return:
        """
        raise NotImplementedError





