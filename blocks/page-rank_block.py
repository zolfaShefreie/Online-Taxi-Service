from abc import ABC
import pandas as pd
import uuid
import datetime
import os

from blocks.base_classes import BaseBlock, BlockType
from settings import RAW_DATA_PATH, SORTED_DATA_PATH


class PageRankBlock(BaseBlock, ABC):

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
            pass

    def _erase_position_ids(self):
        """
        if two data is arrived after all processes ids must set as None
        :return:
        """
        if self.start_data_id is not None and self.end_data_id is not None:
            self.start_data_id = None
            self.end_data_id = None

    def _produce_answer(self, entry_data):
        """
        setup the object properties and apply page-rank to data
        :param entry_data:
        :return:
        """

        self._erase_position_ids()

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
                if self.vertices and self.edges:
                    self._send_data(data=produced_data)

        else:
            pass