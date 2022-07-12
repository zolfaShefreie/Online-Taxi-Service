from abc import ABC

from blocks.base_classes import BaseBlock, BlockType
from settings import KEYSPACE_NAME, CASSANDRA_HOST, CASSANDRA_PORT


class CountPredictorBlock(BaseBlock, ABC):
    KEYSPACE_NAME = KEYSPACE_NAME
    WEEK_TABLE = "week_table"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)

    def _produce_answer(self, entry_data):
        """
        convert row to dict, get timestamp and key
        :param entry_data:
        :return:
        """
        # config = {
        #     "spark.cassandra.connection.host": CASSANDRA_HOST,
        #     "table"
        # }
        self.spark_session.read.format("org.apache.spark.sql.cassandra")\
            .options(table=self.WEEK_TABLE, keyspace=self.KEYSPACE_NAME.lower())\
            .option("spark.cassandra.connection.host", CASSANDRA_HOST).\
            load().show()
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
