from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql.functions import from_json, to_json, struct
import json
import enum


class BlockType(enum.Enum):
    normal = 1
    spark = 2


class BaseBlock:

    def __init__(self, producer_topic: str, bootstrap_servers: str, consumer_topic: str = None,
                 consumer_group_id: str = None, block_type: BlockType = BlockType.normal, spark_session=None,
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
        self._validate_parameter(producer_topic, consumer_topic, bootstrap_servers, consumer_group_id,
                                 block_type, spark_session, *args, **kwargs)
        self.producer_topic = producer_topic
        self.consumer_topic = consumer_topic
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group_id = consumer_group_id
        self.block_type = block_type
        self.spark_session = spark_session

        self.consumer_df = None
        self.producer = None
        self.consumer = None

        run_functions = {BlockType.normal: self._normal_run, BlockType.spark: self._spark_run}
        self.run = run_functions[block_type]

    @staticmethod
    def _entry_value_serializer(data):
        """
        serialize the data
        :param data: string form of data
        :return: new format of data
        """
        return json.dumps(data).encode('utf-8')

    @staticmethod
    def _get_value_schema():
        """
        in spark with have one column value that have json type. this method return schema of value column
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def _get_value_columns():
        """
        :return: a list of value columns that used to convert to json
        """
        raise NotImplementedError

    def _normal_run(self):
        """
        run normal block
        Example Implementation:
            self._normal_setup()
            if self.consumer:
                for each in self.consumer:
                    result = self._produce_answer(each)
                    self._send_data(result)

            else:
                get data from some where and sent result
        :return:
        """
        raise NotImplementedError

    def _spark_run(self):
        """
        run spark block
        Example Implementation:
            df = self._spark_entry_setup()
            df_result = df after some process on df
            self._spark_output_setup()
        :return:
        """
        raise NotImplementedError

    def _spark_entry_setup(self):
        """
        get entry spark dataframe form kafka channel
        :return: df from kafka
        """

        self.consumer_df = self.spark_session.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("kafka.max.poll.records", 10) \
            .option("maxOffsetsPerTrigger", 10) \
            .option("subscribe", self.consumer_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        self.consumer_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        return self.consumer_df

    def _spark_output_setup(self, result_df):
        """
        put stream result_df to kafka
        :param result_df:
        :return:
        """
        # TODO: check can cast as json? if can't, find solution
        col_list = self._get_value_columns()
        # result_df['value'] = result_df[col_list].to_dict(orient='records')
        # result_df.withColumn("value", to_json(struct([x for x in col_list])))\
        #          .drop(*col_list) \
        r_df = result_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                 .writeStream \
                 .format("kafka") \
                 .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                 .option("topic", self.producer_topic) \
                 .option("checkpointLocation", "checkpoint") \
                    .trigger(processingTime='2 seconds') \
                    .outputMode("update") \
            .start().awaitTermination()

        # self.spark_session.streams.awaitAnyTermination()

    def _normal_setup(self):
        """
        normal initial setup
        :return:
        """
        # TODO: check auto_offset_reset
        self.consumer = KafkaConsumer(self.consumer_topic, bootstrap_servers=self.bootstrap_servers,
                                      auto_offset_reset='earliest',
                                      group_id=self.consumer_group_id) if self.consumer_topic else None
        
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer=self._entry_value_serializer)

    def _produce_answer(self, entry_data):
        """
        this function do some process on entry_data and return answer for self._send_data arguments
        :param entry_data:
        :return:
        """
        raise NotImplementedError

    def _send_data(self, data, key=None, headers=None, partition=None, timestamp_ms=None):
        """
        send data via producer to specific topic
        :param data:
        :return:
        """
        self.producer.send(self.producer_topic, key=key, value=data, headers=headers, partition=partition,
                           timestamp_ms=timestamp_ms)

    @staticmethod
    def _validate_parameter(producer_topic, consumer_topic, bootstrap_servers, consumer_group_id, block_type,
                            spark_session, *args, **kwargs):
        """
        validate parameters of object
        :param producer_topic:
        :param consumer_topic:
        :param bootstrap_servers:
        :param consumer_group_id:
        :param block_type:
        :param spark_session:
        :param args:
        :param kwargs:
        :return:
        """
        if (not producer_topic) or (not bootstrap_servers):
            raise Exception("invalid value for primary parameters")
        if block_type == BlockType.spark:
            if spark_session is None:
                raise Exception("the spark block must have spark session as parameter")

        elif block_type == BlockType.normal:
            pass

        else:
            raise Exception("invalid block type")
