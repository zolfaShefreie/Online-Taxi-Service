from abc import ABC
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from pyspark.sql.functions import lit, col
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
# from pyspark.streaming.kafka import KafkaUtils

from blocks.base_classes import BaseBlock, BlockType
from settings import SAVED_TRANSFORMERS_PATH

# from utils.cluster_methods import StructuredStreamingKMeans


class PreTrainedClusteringBlock(BaseBlock, ABC):
    CLUSTER_NUM = 7
    SAVED_MODEL_PATH = SAVED_TRANSFORMERS_PATH + "/" + "kmeans_model"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.spark, *args, **kwargs)
        # self.cluster_model = StreamingKMeans(k=self.CLUSTER_NUM, timeUnit='points').setRandomCenters(5, 100.0, 42)
        self.assemble = VectorAssembler(inputCols=['Lat', 'Lon'], outputCol='features')
        self.scaler = StandardScaler(inputCol='features', outputCol='standardized')
        self.kmeans_model = KMeansModel.load(self.SAVED_MODEL_PATH)

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
        return ['Lat', 'Lon', 'Base', 'Date/Time', 'Cluster']

    def _produce_answer(self, entry_data):
        """
        preprocessing and predict
        predict based on model
        :param entry_data:
        :return:
        """
        assembled_data = self.assemble.transform(entry_data)
        # data_scale = self.scaler.fit(assembled_data)
        # data_scale_output = data_scale.transform(assembled_data)
        data_scale_output = assembled_data.withColumn("standardized", col("features"))
        return self.kmeans_model.transform(data_scale_output)

    def _spark_run(self):
        df = self._spark_entry_setup()
        result_df = self._produce_answer(df)
        result_df = result_df.withColumn("Cluster", col("prediction"))
        self._spark_output_setup(result_df)
