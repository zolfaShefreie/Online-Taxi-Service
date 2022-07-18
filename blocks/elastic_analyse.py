from abc import ABC
from ast import Return
import json
# from typing import final
# from unittest import result
from elasticsearch import Elasticsearch
import datetime
# import uuid

from blocks.base_classes import BaseBlock, BlockType
from settings import ELASTIC_SERVER, ELASTIC_PASSWORD


class ElasticAnalyseBlock(BaseBlock, ABC):

    es = Elasticsearch(hosts=ELASTIC_SERVER, verify_certs=False,
                       basic_auth=("elastic", ELASTIC_PASSWORD) if ELASTIC_PASSWORD else None,
                       timeout=30)
    DATETIME_FORMAT = "%m/%d/%Y %H:%M:%S"

    def __init__(self, index_name="taxi_service_index", *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)
        self.index_name = index_name

    def _generate_index(self):
        """
        generate elastic index if it does not exist
        :return:
        """
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings={
                "properties": {
                    "Date/Time": {"type": "date", "format": "MM/dd/yyyy HH:mm:ss", "index": "true", "store": "true"},
                    "Lat": {"type": "float", "index": "true", "store": "true"},
                    "Lon": {"type": "float", "index": "true", "store": "true"},
                    "Base": {"type": "text", "index": "true", "store": "true"},
                    "Location": {"type": "geo_point", "index": "true", "store": "true"},
                    "Cluster_number": {"type": "integer", "index": "true", "store": "true"}
                }
            })

    def _produce_answer(self, entry_data, data_id=0):
        """
        save row in elasticsearch
        :param entry_data:
        :param data_id: document id
        :return:
        """

        # convert class byte to dictionary
        consumer_value = json.loads(entry_data.value.decode('utf-8'))

        self.es.index(
            index='taxi_service_index',
            id=str(data_id),
            document={
                'Date/Time': datetime.datetime.strptime(consumer_value['Date/Time'],
                                                        self.DATETIME_FORMAT).strftime(self.DATETIME_FORMAT),
                'Lat': consumer_value['Lat'],
                'Lon': consumer_value['Lon'],
                'Base': consumer_value['Base'],
                'Cluster_number': 0,
                "Location": {
                    "lat": consumer_value['Lat'],
                    "lon": consumer_value['Lon']
                }
            })

        key = str.encode(entry_data.key.decode("utf-8"))
        timestamp = entry_data.timestamp
        return consumer_value, key, timestamp
        

    def _normal_run(self):
        """
        run method for normal type
            create normal
        :return:
        """
        self._generate_index()
        self._normal_setup()
        data_id = 0
 
        if self.consumer:
            for each in self.consumer:
                produced_data, key, timestamp = self._produce_answer(each, data_id=data_id)
                data_id += 1
                self._send_data(data=produced_data, key=key, timestamp_ms=timestamp)

        else:
            print("No data in previous phase topic")


