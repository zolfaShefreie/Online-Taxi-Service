from abc import ABC
import json
from elasticsearch import Elasticsearch

from blocks.base_classes import BaseBlock, BlockType
from settings import ELASTIC_SERVER, ELASTIC_PASSWORD


class ElasticAnalyseBlock(BaseBlock, ABC):
    es = Elasticsearch(hosts=ELASTIC_SERVER, verify_certs=False, basic_auth=("elastic", ELASTIC_PASSWORD), timeout=30)
    data_dic = dict()
    bulk_data = list()

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)

    def _generate_index(self):
        self.es.indices.create(index="taxi_service_index", mappings={
            "properties": {
                "Date/Time": {"type": "date", format: "MM/dd/yyyy HH:mm:ss", "index": "true", "store": "true"},
                "Lat": {"type": "float", "index": "true", "store": "true"},
                "Lon": {"type": "float", "index": "true", "store": "true"},
                "Base": {"type": "text", "index": "true", "store": "true"},
                "Cluster_number": {"type": "integer", "index": "true", "store": "true"}
            }
        })

    def _produce_answer(self, entry_data):
        """
        save row in elasticsearch
        :param entry_data:
        :return:
        """
        # convert class byte to dictionary
        consumer_value = json.loads(entry_data.value.decode('utf-8'))

        self.es.index(
            index='taxi_service_index',
            document={
                'Date/Time': consumer_value['Date/Time'],
                'Lat': consumer_value['Lat'],
                'Lon': consumer_value['Lon'],
                'Base': consumer_value['Base'],
                'Cluster_number': 0
            })

    def _normal_run(self):
        """
        run method for normal type
            create normal
        :return:
        """
        self._generate_index()
        self._normal_setup()
        if self.consumer:
            for each in self.consumer:
                self._produce_answer(each)
                #self._send_data(result)

        else:
            print("No data in previous phase topic")
