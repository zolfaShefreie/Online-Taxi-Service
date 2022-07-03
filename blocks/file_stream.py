from abc import ABC
import pandas as pd

from blocks.base_classes import BaseBlock, BlockType
from settings import RAW_DATA_PATH, SORTED_DATA_PATH


class FileStreamBlock(BaseBlock, ABC):
    FILE_PATH = RAW_DATA_PATH if RAW_DATA_PATH else SORTED_DATA_PATH
    AFTER_BASIC_PROCESS_FILE_PATH = SORTED_DATA_PATH
    SORT_DATA = True if RAW_DATA_PATH else False
    SORT_BY = 'Date/Time'

    def __init__(self, *args, **kwargs):
        super().__init__(consumer_topic=None, block_type=BlockType.normal)

    def _produce_answer(self, entry_data):
        """
        convert row to dict
        :param entry_data:
        :return:
        """
        data = dict()
        data['Date/Time'] = entry_data['Date/Time']
        data['Lat'] = entry_data['Lat']
        data['Lon'] = entry_data['Lon']
        data['Base'] = entry_data['Base']
        return data

    def _normal_run(self):
        self._normal_setup()
        df = pd.read_csv(self.FILE_PATH, header=[0])
        if self.SORT_DATA:
            df.sort_values(by="Date/Time")
            df.to_csv(self.AFTER_BASIC_PROCESS_FILE_PATH, sep=',', encoding='utf-8', index=False)

        key = 0
        for row in range(len(df)):
            key += 1
            produced_data = self._produce_answer(df.loc[row].to_dict())
            self._send_data(produced_data, key=str(key))
