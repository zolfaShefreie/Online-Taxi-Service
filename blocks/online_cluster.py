from abc import ABC
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.sql.types import StructType, StructField, FloatType, StringType

from blocks.base_classes import BaseBlock, BlockType


class OnlineClusteringBlock(BaseBlock, ABC):
    CLUSTER_NUM = 11

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.spark, *args, **kwargs)
        self.cluster_model = StreamingKMeans(k=self.CLUSTER_NUM, timeUnit='points')

    @staticmethod
    def _get_value_schema():
        """
        in spark with have one column value that have json type. this method return schema of value column
        :return:
        """
        return StructType(
                [
                    StructField('Date/Time', StringType(), True),
                    StructField('Lat', FloatType(), True),
                    StructField('Lon', FloatType(), True),
                    StructField('Base', StringType(), True),
                ]
            )

    @staticmethod
    def _get_value_columns():
        """
        :return: a list of value columns that used to convert to json
        """
        return ['Lat', 'Lon', 'Base', 'Date/Time']

    def _produce_answer(self, entry_data):
        """
        predict based on model
        :param entry_data:
        :return:
        """
        return self.cluster_model.predictOn(entry_data)

    def _spark_run(self):
        df = self._spark_entry_setup()
        # cluster_train_data = df.select('Lat', 'Lon')
        # self.cluster_model.trainOn(cluster_train_data)
        self._spark_output_setup(df)


