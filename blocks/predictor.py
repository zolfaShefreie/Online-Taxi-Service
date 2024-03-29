from abc import ABC
import pyspark.sql.functions as F
from prophet import Prophet
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType
import pandas as pd
import os
import json

from blocks.base_classes import BaseBlock, BlockType
from settings import KEYSPACE_NAME, CASSANDRA_HOST, CASSANDRA_PORT


class CountPredictorBlock(BaseBlock, ABC):
    KEYSPACE_NAME = KEYSPACE_NAME
    WEEK_TABLE_NAME = "week_table"
    HALF_DAY_TABLE_NAME = "midday_table"
    MONTH_TABLE_NAME = "month_table"
    DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
    MIN_NUMBER_START_TRAIN = {'half_day': 20, 'week': 5, 'month': 4}
    TRAIN_WAIT_STEP = {'half_day': 20, 'week': 1, 'month': 1}
    TEST_SPLIT = {'half_day': 0.2, 'week': 1/5, 'month': 1/3}
    RESULT_SCHEMA = StructType([
        StructField('ds', TimestampType()),
        StructField('y', IntegerType()),
        StructField('yhat', DoubleType()),
        StructField('yhat_upper', DoubleType()),
        StructField('yhat_lower', DoubleType())
    ])
    SAVE_IMAGE_PATH = "./predictor_images"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)
        self.last_train_index = {'half_day': 0, 'week': 0, 'month': 0}

    def _visualize(self, df, sub_dir_name, kind="week", model=None):
        """
        visualize the model prediction and save it
        :param df:
        :param sub_dir_name:
        :param kind:
        :param model:
        :return:
        """
        if not os.path.exists(f"{self.SAVE_IMAGE_PATH}/{sub_dir_name}/_{self.last_train_index[kind]}"):
            os.makedirs(f"{self.SAVE_IMAGE_PATH}/{sub_dir_name}/_{self.last_train_index[kind]}")

        df = df.toPandas()
        df_1 = df.set_index('ds')
        plot = df_1[['y', 'yhat']].plot()
        fig = plot.get_figure()
        fig.savefig(f"{self.SAVE_IMAGE_PATH}/{sub_dir_name}/_{self.last_train_index[kind]}/compare.png")
        model.plot(df).savefig(f"{self.SAVE_IMAGE_PATH}/{sub_dir_name}/_{self.last_train_index[kind]}/model_plot.png")
        model.plot_components(df).savefig(f"{self.SAVE_IMAGE_PATH}/{sub_dir_name}/_{self.last_train_index[kind]}/components.png")

    def _get_permission(self, df_count, kind="week") -> bool:
        """
        check can start new fit or not
        :param df_count:
        :param kind: half_day, week, month
        :return:
        """
        limit = (self.last_train_index[kind] * self.TRAIN_WAIT_STEP[kind]) + self.MIN_NUMBER_START_TRAIN[kind]
        return df_count >= limit

    def _models_management(self, half_day_df, week_df, month_df):
        """
        check that have learning condition and fit the model
        :param half_day_df:
        :param week_df:
        :param month_df:
        :return:
        """
        if self._get_permission(half_day_df.count(), 'half_day'):
            half_day_model = Prophet(seasonality_mode='multiplicative', changepoint_prior_scale=0.01,
                                     yearly_seasonality=True)
            half_day_model.add_seasonality(name='monthly', period=30, fourier_order=5)
            train_data = half_day_df.limit(int(half_day_df.count() * (1 - self.TEST_SPLIT['half_day'])))
            train_data.show()
            # test_data = half_day_df.subtract(train_data)
            result = self._predict(train_data.toPandas(), half_day_df.toPandas(), half_day_model)
            self.last_train_index['half_day'] = self.last_train_index['half_day'] + 1
            self._visualize(result, 'half_day', 'half_day', half_day_model)

        if self._get_permission(week_df.count(), 'week'):
            week_model = Prophet(seasonality_mode='multiplicative', yearly_seasonality=True)
            train_data = week_df.limit(int(week_df.count() * (1 - self.TEST_SPLIT['week'])))
            # test_data = week_df.subtract(train_data)
            result = self._predict(train_data.toPandas(), week_df.toPandas(), week_model)
            self.last_train_index['week'] = self.last_train_index['week'] + 1
            self._visualize(result, 'week', 'week', week_model)

        if self._get_permission(month_df.count(), 'month'):
            month_model = Prophet(seasonality_mode='multiplicative', yearly_seasonality=True)
            train_data = month_df.limit(int(month_df.count() * (1 - self.TEST_SPLIT['month'])))
            # test_data = month_df.subtract(train_data)
            result = self._predict(train_data.toPandas(), month_df.toPandas(), month_model)
            self.last_train_index['month'] = self.last_train_index['month'] + 1
            self._visualize(result, 'month', 'month', month_model)

    def _predict(self, pd_train_df, pd_df, model):
        """
        fit model and make future data to predict
        :param pd_train_df:
        :param model:
        :param pd_df:
        :return:
        """

        model.fit(pd_train_df)
        forecast_pd = model.predict(pd_df)
        merge_result = pd.merge(forecast_pd, pd_df, on='ds')
        return self.spark_session.createDataFrame(merge_result)

    def _pre_process_dataframe(self, df, key_col_name):
        """
        pre_process data based on input of model.fit
        :param df:
        :param key_col_name:
        :return:
        """
        if df.count() != 0:
            df = df.withColumn("y", F.size(F.col("date_time"))) \
                   .select(key_col_name, 'y') \
                   .withColumn("ds", F.to_timestamp(F.col(key_col_name)['0'], self.DEFAULT_DATETIME_FORMAT))\
                   .select("ds", "y") \
                   .sort(F.asc("ds"))
            return self.spark_session.createDataFrame(df.take(df.count()))
        return df

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
        half_day_df = self._pre_process_dataframe(self._read_from_cassandra(self.HALF_DAY_TABLE_NAME), key_col_name="hour")
        week_df = self._pre_process_dataframe(self._read_from_cassandra(self.WEEK_TABLE_NAME), key_col_name="week")
        month_df = self._pre_process_dataframe(self._read_from_cassandra(self.MONTH_TABLE_NAME), key_col_name="month")
        self._models_management(half_day_df, week_df, month_df)
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
                self._produce_answer(each)
                self._send_data(data=json.loads(each.value.decode('utf-8')), key=each.key, timestamp_ms=each.timestamp)
        else:
            pass
