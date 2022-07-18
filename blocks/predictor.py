from abc import ABC
import pyspark.sql.functions as F
from prophet import Prophet
from pyspark.ml.feature import VectorAssembler

from blocks.base_classes import BaseBlock, BlockType
from settings import KEYSPACE_NAME, CASSANDRA_HOST, CASSANDRA_PORT


class CountPredictorBlock(BaseBlock, ABC):
    KEYSPACE_NAME = KEYSPACE_NAME
    TABLE_NAME = "uuid_table"
    WEEK_TABLE_NAME = "week_table"
    HALF_DAY_TABLE_NAME = "midday_table"
    MONTH_TABLE_NAME = "month_table"
    DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
    MAX_NUMBER_START_TRAIN = 5
    TEST_SIZE = 1

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)

    def _pre_process_dataframe(self, df, key_col_name):
        """
        pre_process data based on input of model.fit
        :param df:
        :param key_col_name:
        :return:
        """
        df = df.withColumn("y", F.size(F.col("date_time"))) \
               .select(key_col_name, 'y') \
               .withColumn("ds", F.to_timestamp(F.col(key_col_name)['0'], self.DEFAULT_DATETIME_FORMAT))\
               .select("ds", "y") \
               .sort(F.asc("count"))
        return self.spark_session.createDataFrame(df.take(df.count()))

    def _read_from_cassandra(self, table_name):
        """
        read table from cassandra via using spark
        :param table_name:
        :return:
        """
        return self.spark_session.read.format("org.apache.spark.sql.cassandra") \
                   .options(table=table_name, keyspace=self.KEYSPACE_NAME.lower()) \
                   .option("spark.cassandra.connection.host", CASSANDRA_HOST) \
                   .load()

    def _produce_answer(self, entry_data):
        """
        convert row to dict, get timestamp and key
        :param entry_data:
        :return:
        """
        half_day_df = self._pre_process_dataframe(self._read_from_cassandra(self.WEEK_TABLE_NAME), key_col_name="hour")
        week_df = self._pre_process_dataframe(self._read_from_cassandra(self.WEEK_TABLE_NAME), key_col_name="week")
        month_df = self._pre_process_dataframe(self._read_from_cassandra(self.WEEK_TABLE_NAME), key_col_name="month")
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
