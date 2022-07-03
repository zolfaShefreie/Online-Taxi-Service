from kafka import KafkaProducer, KafkaConsumer
import json
import enum


class BlockType(enum.Enum):
    normal = 1
    spark = 2


class BaseBlock:
    
    def __init__(self,  producer_topic: str, consumer_topic: str, bootstrap_servers: str, consumer_group_id: str,
                 block_type: BlockType=BlockType.normal, spark_session=None,
                 *args, **kwargs):
        """
        initial function for each block.
        :param producer_topic: the name of topic that produce sth
        :param consumer_topic: the name of topic that is used in this black
        :param bootstrap_servers:
        :param consumer_group_id:
        :param block_type:
        :param spark_session:
        :param args:
        :param kwargs:
        """
        self.validate_parameter(producer_topic, consumer_topic, bootstrap_servers, consumer_group_id,
                                block_type, spark_session, *args, **kwargs)
        self.producer_topic = producer_topic
        self.consumer_topic = consumer_topic
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group_id = consumer_group_id
        self.block_type = block_type
        self.spark_session = spark_session

        self.consumer_df = None

        # self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
        #                               value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        #
        # # TODO: check auto_offset_reset
        # self.consumer = KafkaConsumer(consumer_topic, bootstrap_servers=bootstrap_servers,
        #                               auto_offset_reset='earliest',
        #                               group_id=consumer_group_id) if consumer_topic else None

    def spark_entry_setup(self):
        """
        get entry spark dataframe form kafka channel
        :return: df from kafka
        """

        self.consumer_df = self.spark_session.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.consumer_topic) \
            .load()
        # TODO: check can cast as json? if can't, find solution
        self.consumer_df.selectExpr("CAST(key AS STRING)", "CAST(value AS JSON)")

        return self.consumer_df

    def spark_output_setup(self, result_df):
        """
        put stream result_df to kafka
        :param result_df:
        :return:
        """
        # TODO: check can cast as json? if can't, find solution
        result_df.selectExpr("CAST(key AS STRING)", "CAST(value AS JSON)") \
                 .write \
                 .format("kafka") \
                 .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                 .option("topic", self.producer_topic) \
                 .save()

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

    @staticmethod
    def validate_parameter(producer_topic, consumer_topic, bootstrap_servers, consumer_group_id, block_type,
                           spark_session, param, param1):
        if block_type == BlockType.spark:
            if spark_session is None:
                raise Exception("the spark block must have spark session as parameter")

        if block_type == BlockType.normal:
            pass

        else:
            raise Exception("invalid block type")
