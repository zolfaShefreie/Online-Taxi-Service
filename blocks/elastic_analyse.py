from abc import ABC
from ast import Return
import json
from typing import final
from unittest import result
from elasticsearch import Elasticsearch
import datetime
import uuid

from blocks.base_classes import BaseBlock, BlockType
from settings import ELASTIC_SERVER, ELASTIC_PASSWORD, ELASTIC_STARTTIME, ELASTIC_ENDTIME


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
    # this method runs a query to get 

    def _timespan_query(self, point_number = 10):
        query = {
            "aggs": {
                "filter_date":{
                    "filter":{
                        "range":{
                            "Date/Time": {
                                "gte": ELASTIC_STARTTIME,
                                "lte": ELASTIC_ENDTIME
                            }
                            
                        }
                    },
                    "aggs": {
                        "geo":{
                            "geohash_grid": {
                                "field": "Location",
                                "precision":7,
                                "size":10
                            },
                            "aggs": {
                                "point": {
                                    "geo_centroid": {
                                        "field": "Location"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        term_results = self.es.search(index="taxi_service_index", body=query)

        result_list = []
        for i in range(0, 10):
            result_dict = {}
            result_dict['Date/Time'] = json.dumps(term_results["hits"]['hits'][i]["_source"]['Date/Time'])
            result_dict['Lat'] = json.dumps(term_results["hits"]['hits'][i]["_source"]['Lat'])
            result_dict['Lon'] = json.dumps(term_results["hits"]['hits'][i]["_source"]['Lon'])
            result_dict['Base'] = json.dumps(term_results["hits"]['hits'][i]["_source"]['Base'])
            result_dict['Location'] = json.dumps(term_results["hits"]['hits'][i]["_source"]['Location'])
            result_dict['Query'] = 'time span query'
            result_list.append(result_dict)

        print("query 3")

        return result_list



    def _get_last_nstreams_query(self):
        query={"query": { "match_all" : {}}}

        resp = self.es.search(index='taxi_service_index', body=query, sort = [{"Date/Time":{"order":"desc"}}], size=100)

        hits = resp['hits']
        response_list = hits['hits']

        final_results = []
        for each in response_list:
            result = each['_source']
            result['query'] = 'n last streamed data'
            final_results.append(result)
            # print('Date/Time:', result['Date/Time'], " Latitude:", result['Lat'], " Longitude:", result['Lon'])
        print("query 1")

        return final_results
    
    def _dense_locations_query(self, point_number = 10):
        query = {
            "aggs":{
                "geo":{
                    "geohash_grid":{
                        "field": "Location",
                        "precision": 7,
                        "size":10
                    },
                    "aggs":{
                        "point": {
                            "geo_centroid": {
                                "field": "Location"
                            }
                        }
                    }
                }
            }
        }

        term_results = self.es.search(index="taxi_service_index", body=query)

        final_results = []
        for i in range(0, point_number):
            result_dict = {}
            result_dict['Lat'] = json.dumps(term_results["aggregations"]["geo"]["buckets"][i]["point"]["location"]['lat'])
            result_dict['Lon'] = json.dumps(term_results["aggregations"]["geo"]["buckets"][i]["point"]["location"]['lon'])
            result_dict['Count'] = json.dumps(term_results["aggregations"]["geo"]["buckets"][i]["point"]["count"])
            result_dict['Query'] = "n most dense locations"
            final_results.append(result_dict)
        
        print("query2")
        return final_results



    def _produce_answer(self, entry_data, data_id=0, type='non_query'):
        """
        save row in elasticsearch
        :param entry_data:
        :param data_id: document id
        :return:
        """
        if type == 'non_query':
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
        else:
            key = str.encode(str(uuid.uuid4()))
            now = datetime.datetime.now()
            now = now.strftime(self.DATETIME_FORMAT)
            now = str(now)
            date_time_str = datetime.datetime.strptime(now, self.DATETIME_FORMAT)
            timestamp = int(datetime.datetime.timestamp(date_time_str))

            return entry_data, key, timestamp

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
                self._produce_answer(each, data_id=data_id)
                data_id += 1
                #self._send_data(result)

        else:
            print("No data in previous phase topic")

        last_stream_query_list = self._get_last_nstreams_query()
        for each in last_stream_query_list:
            produced_data, key, timestamp = self._produce_answer(each, type='query')
            self._send_data(data=produced_data, key=key, timestamp_ms=timestamp)

        dense_locations_query_list = self._dense_locations_query()
        for each in dense_locations_query_list:
            produced_data, key, timestamp = self._produce_answer(each, type='query')
            self._send_data(data=produced_data, key=key, timestamp_ms=timestamp)
        
        timespan_query_list = self._timespan_query()
        for each in timespan_query_list:
            produced_data, key, timestamp = self._produce_answer(each, type='query')
            self._send_data(data=produced_data, key=key, timestamp_ms=timestamp)

