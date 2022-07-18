from abc import ABC
from graphframes import GraphFrame
import json

from blocks.base_classes import BaseBlock, BlockType


class PageRankBlock(BaseBlock, ABC):

    MAX_NUM_RESULT = 3

    def __init__(self, *args, **kwargs):
        super().__init__(consumer_topic=None, block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)
        self.start_data_id = None
        self.end_data_id = None
        self.vertices = None
        self.edges = None

    def _set_edges(self, data_id):
        """
        set request_id as start_data_id or end_data_id and add edges
        :param data_id:
        :return:
        """
        self.start_data_id = data_id if self.start_data_id is None else self.start_data_id
        self.end_data_id = data_id if (self.start_data_id is not None) and \
                                      (self.end_data_id is None) else self.end_data_id

        if self.start_data_id is not None and self.end_data_id is not None:
            value = {'src': self.start_data_id, 'dst': self.end_data_id}
            df = self.spark_session.createDataFrame([value, ])
            self.edges = df if self.edges is None else self.edges.union(df)

    def _erase_position_ids(self):
        """
        if two data is arrived after all processes ids must set as None
        :return:
        """
        if self.start_data_id is not None and self.end_data_id is not None:
            self.start_data_id = None
            self.end_data_id = None

    def _page_rank(self) -> list:
        """
        create graph based on vertices and edges and
        :return:
        """
        if self.edges is None or self.vertices is None:
            return None
        graph = GraphFrame(self.vertices, self.edges)
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

        # add to vertices and edges
        df = self.spark_session.createDataFrame([value, ]).select('Lat', 'Lon')
        self.vertices = df if self.edges is None else self.edges.union(df)
        self._set_edges(key)

        self._erase_position_ids()
        # page_rank result
        return self._page_rank()

    def _normal_run(self):
        """
        run method for normal type
            create normal
        :return:
        """
        self._normal_setup()
        if self.consumer:
            for each in self.consumer:
                produced_data = self._produce_answer(each)
                if self.vertices and self.edges and produced_data:
                    self._send_data(data=produced_data)

        else:
            pass