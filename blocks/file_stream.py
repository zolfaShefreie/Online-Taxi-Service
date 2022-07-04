from abc import ABC
import pandas as pd
import uuid
import datetime

from blocks.base_classes import BaseBlock, BlockType
from settings import RAW_DATA_PATH, SORTED_DATA_PATH


class FileStreamBlock(BaseBlock, ABC):
    FILE_PATH = RAW_DATA_PATH if RAW_DATA_PATH else SORTED_DATA_PATH
    AFTER_BASIC_PROCESS_FILE_PATH = SORTED_DATA_PATH
    SORT_DATA = True if RAW_DATA_PATH else False
    SORT_BY = 'Date/Time'
    DATETIME_COL = 'Date/Time'
    DATETIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"

    def __init__(self, *args, **kwargs):
        super().__init__(consumer_topic=None, block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)

    def _produce_answer(self, entry_data):
        """
        convert row to dict, get timestamp and key
        :param entry_data:
        :return:
        """
        key = str(uuid.uuid4())
        element = datetime.datetime.strptime(entry_data[self.DATETIME_COL], self.DATETIME_FORMAT)
        timestamp = datetime.datetime.timestamp(element)
        return entry_data.to_dict(), key, timestamp

    def _normal_run(self):
        self._normal_setup()
        df = pd.read_csv(self.FILE_PATH, header=[0])
        if self.SORT_DATA:
            df.sort_values(by=self.SORT_BY)
            df.to_csv(self.AFTER_BASIC_PROCESS_FILE_PATH, sep=',', encoding='utf-8', index=False)

        for row in range(len(df)):
            produced_data, key, timestamp = self._produce_answer(df.loc[row])
            self._send_data(data=produced_data, key=key, timestamp_ms=timestamp)
