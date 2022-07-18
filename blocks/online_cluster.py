from abc import ABC
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from pyspark.sql.functions import lit, col
from pyspark.ml.clustering import KMeansModel, KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler, StandardScalerModel
import json

from blocks.base_classes import BaseBlock, BlockType
from settings import SAVED_TRANSFORMERS_PATH
from utils.cluster_methods import OnlineKMeans


class PreTrainedClusteringBlock(BaseBlock, ABC):
    CLUSTER_NUM = 7
    SAVED_MODEL_PATH = SAVED_TRANSFORMERS_PATH + "/" + "kmeans_model_7"
    SCALER_MODEL_PATH = SAVED_TRANSFORMERS_PATH + "/" + "scaler"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.spark, *args, **kwargs)
        self.assemble = VectorAssembler(inputCols=['Lat', 'Lon'], outputCol='features')
        self.scaler = StandardScalerModel.load(self.SCALER_MODEL_PATH)
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
        data_scale_output = self.scaler.transform(assembled_data)
        return self.kmeans_model.transform(data_scale_output)

    def _spark_run(self):
        df = self._spark_entry_setup()
        result_df = self._produce_answer(df)
        result_df = result_df.withColumn("Cluster", col("prediction"))
        self._spark_output_setup(result_df)


class OnlineClusteringBlock(BaseBlock, ABC):
    CLUSTER_NUM = 7
    COlUMN_NAME = ['Lat', 'Lon']
    ASSEMBLER_OUTPUT = "features"
    SCALE_OUTPUT = "standardized"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, *args, **kwargs)
        self.train_data = None
        self.kmeans_model = KMeans(featuresCol=self.SCALE_OUTPUT, k=self.CLUSTER_NUM)
        self.assembler = VectorAssembler(inputCols=self.COlUMN_NAME, outputCol=self.ASSEMBLER_OUTPUT)
        self.scale = StandardScaler(inputCol=self.ASSEMBLER_OUTPUT, outputCol=self.SCALE_OUTPUT)

        # self.cluster_model = OnlineKMeans(k=self.CLUSTER_NUM, timeUnit='points').setRandomCenters(5, 100.0, 42)

    @staticmethod
    def _get_value_columns():
        """
        :return: a list of value columns that used to convert to json
        """
        return ['Lat', 'Lon', 'Base', 'Date/Time', 'Cluster']

    def _pre_processed(self, train_dataframe, test_dataframe):
        """
        pre_processed dataframes:
            1.convert to vector
            2.standard Scaler
        :param train_dataframe:
        :param test_dataframe:
        :return: preprocessed_model
        """
        assembled_train_data = self.assembler.transform(train_dataframe)
        scale_model = self.scale.fit(assembled_train_data)
        assembled_test_data = self.assembler.transform(test_dataframe)
        return scale_model.transform(assembled_train_data), scale_model.transform(assembled_test_data)

    def _produce_answer(self, entry_data):
        """
        this function do some process on entry_data and return answer for self._send_data arguments
            1.convert data to rdd
            2.train model
            3.predict
            4.convert to correct format to send to kafka topic
        :param entry_data:
        :return:
        """
        value = json.loads(entry_data.value.decode('utf-8'))
        if self.train_data is None:
            self.train_data = test_df = self.spark_session.createDataFrame([value, ])
        else:
            test_df = self.spark_session.createDataFrame([value, ])
            self.train_data = self.train_data.union(test_df)

        # preprocess and train model to get prediction
        preprocessed_train_df, preprocessed_test_df = self._pre_processed(self.train_data, test_df)
        model = self.kmeans_model.fit(preprocessed_train_df)
        result_df = model.transform(preprocessed_test_df).withColumn("Cluster", col("prediction"))\
            .select(self._get_value_columns())

        # convert to dictionary
        value_plus_cluster = list(map(lambda row: row.asDict(), result_df.collect()))
        return value_plus_cluster[0], entry_data.key, entry_data.timestamp

    def _normal_run(self):
        self._normal_setup()
        if self.consumer:
            for each in self.consumer:
                produced_data, key, timestamp = self._produce_answer(each)
                self._send_data(data=produced_data, key=key, timestamp_ms=timestamp)
        else:
            pass
