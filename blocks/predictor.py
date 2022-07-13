from abc import ABC
import pyspark.sql.functions as F
from prophet import Prophet
from pyspark.ml.feature import VectorAssembler

from blocks.base_classes import BaseBlock, BlockType
from settings import KEYSPACE_NAME, CASSANDRA_HOST, CASSANDRA_PORT


class CountPredictorBlock(BaseBlock, ABC):
    KEYSPACE_NAME = KEYSPACE_NAME
    TABLE_NAME = "uuid_table"
    DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)
    
    def _get_count(self, df, end_col_name: str):
        """
        get count of rows that between datetime and end_col_name
        :param df: dataframe
        :param end_col_name: 
        :return: 
        """
        df.join(df.select(F.col('datetime').alias('another_datetime')))\
            .filter(F.col('datetime') > F.col('another_datetime'))\
            .withColumn("one", F.when(F.col('another_datetime').between(F.col("datetime"), F.col(end_col_name)), 1)
                        .otherwise(0))\
            .groupBy("uuid").agg(F.sum("one").alias(f"count_{end_col_name}"))
        return self.spark_session.createDataFrame(df.take(df.count()))

    def _pre_process(self, df):
        """

        :param df:
        :return:
        """
        df = df.withColumn("datetime", F.to_timestamp(F.col("str_date_ime"), self.DEFAULT_DATETIME_FORMAT))
        df = df.withColumn("after_12_hours", (F.unix_timestamp(F.col("str_date_ime")) + 12 * 60 * 60).cast('timestamp'))
        # df = df.withColumn("after_7_day", (F.unix_timestamp(F.col("str_date_ime")) + 7 * 24 * 60 * 60).cast('timestamp'))
        # df = df.withColumn("after_1_month", (F.unix_timestamp(F.col("str_date_ime")) + 30 * 24 * 60 * 60).cast('timestamp'))
        df = self.spark_session.createDataFrame(df.take(df.count()))
        df = self._get_count(df, "after_12_hours")

        # df = self._get_count(df, "after_7_day")
        # df = self._get_count(df, "after_1_month")

    def _produce_answer(self, entry_data):
        """
        convert row to dict, get timestamp and key
        :param entry_data:
        :return:
        """
        df = self.spark_session.read.format("org.apache.spark.sql.cassandra")\
            .options(table=self.TABLE_NAME, keyspace=self.KEYSPACE_NAME.lower())\
            .option("spark.cassandra.connection.host", CASSANDRA_HOST).\
            load()
        self._pre_process(df.select('str_date_ime', 'uuid'))
        # df.printSchema()
        # df.select('str_date_ime').show()
        # print(df.take(1)[0].str_date_ime, "**************************************************")
        return entry_data

    def _normal_run(self):
        """
        run method for normal type
            create normal
        :return:
        """
        self._normal_setup()
        if self.consumer:
            for each in self.consumer:
                result = self._produce_answer(each)
                # self._send_data(result)

        else:
            pass
