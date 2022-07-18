from abc import ABC
from graphframes import GraphFrame
import json
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as F

from blocks.base_classes import BaseBlock, BlockType


class PageRankBlock(BaseBlock, ABC):

    MAX_NUM_RESULT = 3
    MAX_ITER = 10

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)
        self.is_even_data = True
        self.all_data = None
        self.vertices = None

    def _create_edges(self):
        """
        split data and make edge
        :return: edge dataframe
        """
        if self.is_even_data:
            count = self.all_data.count()
            first_data = self.all_data.limit(int(count/2))
            second_data = self.all_data.subtract(first_data)\
                                       .withColumn("index", monotonically_increasing_id())\
                                       .select(F.col("id").alias("dst_id"),
                                               F.col("Lat").alias("dst_lat"),
                                               F.col("Lon").alias("dst_lon"),
                                               F.col("index"))
            first_data = first_data.withColumn("index", monotonically_increasing_id())\
                                   .select(F.col("id").alias("src_id"),
                                           F.col("Lat").alias("src_lat"),
                                           F.col("Lon").alias("src_lon"),
                                           F.col("index"))

            travel_df = first_data.join(second_data, on="index", how="inner")
            return travel_df.select(
                F.col('src_id').alias('src'),
                F.col('dst_id').alias('dst')
            )
        return None

    def _page_rank(self, edges) -> list:
        """
        create graph based on vertices and edges and
        :param: edges:
        :return: a list of dictionary that each dictionary is a point with page_rank score
        """
        if edges is None or self.vertices is None:
            return None
        graph = GraphFrame(self.vertices, edges)
        page_rank_result = graph.pageRank(resetProbability=0.15, tol=0.01) \
                                .vertices \
                                .sort('pagerank', ascending=False) \
                                .select("Lat", "Lon", "pagerank")
        return list(map(lambda row: row.asDict(), page_rank_result.take(self.MAX_NUM_RESULT)))

    def _produce_answer(self, entry_data):
        """
        setup the object properties and apply page-rank to data
        :param entry_data:
        :return:
        """

        # get value and create id based on lat , lon
        value = json.loads(entry_data.value.decode('utf-8'))
        key = f"({value['Lat']}, {value['Lon']})"
        value['id'] = key

        # add to vertices and create edges
        df = self.spark_session.createDataFrame([value, ])
        self.all_data = df if self.all_data is None else self.all_data.union(df)
        self.vertices = df.select('id', 'Lat', 'Lon') if self.vertices is None else self.vertices.union(df.select('id', 'Lat', 'Lon'))
        self.vertices = self.vertices.dropDuplicates()
        edges = self._create_edges()

        # page_rank result
        return self._page_rank(edges)

    def _normal_run(self):
        """
        run method for normal type
            create normal
        :return:
        """
        self._normal_setup()
        if self.consumer:
            for each in self.consumer:
                self.is_even_data = not self.is_even_data
                produced_data = self._produce_answer(each)
                if self.vertices and produced_data:
                    self._send_data(data=produced_data)

        else:
            pass