from abc import ABC
import json
import datetime
import redis

from blocks.base_classes import BaseBlock, BlockType
from settings import REDIS_SERVER as setting_redis_server
from settings import REDIS_PORT as setting_redis_port


class RedisAnalyseBlock(BaseBlock, ABC):
    REDIS_SERVER = setting_redis_server
    REDIS_PORT = setting_redis_port
    DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)
        self.r = redis.Redis(host=self.REDIS_SERVER, port=self.REDIS_PORT, db=0)

    def _separation(self, entry_data):
        self.uuid = entry_data.key.decode('utf-8')
        consumer_value = json.loads(entry_data.value.decode('utf-8'))
        self.current_date = datetime.datetime.strptime(consumer_value['Date/Time'], self.DATETIME_FORMAT)
        self.current_lat = str(consumer_value['Lat'])
        self.current_lon = str(consumer_value['Lon'])
        self.current_base = consumer_value['Base']
        self.current_cluster_number = '0'
        # TODO:self.cluster_numbers.append(int(consumer_value['Cluster_number']))

    def _add(self):
        value = self.uuid + '~' + str(self.current_date) + '~' + self.current_lat + '~' + self.current_lon + '~' +\
                self.current_base + '~' + self.current_cluster_number
        self.r.rpush('day~' + str(self.current_date), value)  # add value to the tail of a Redis list
        self.r.rpush('hour~' + str(self.current_date), value)

    def _delete(self):
        previous_week = (datetime.datetime.combine(self.current_date.date(), self.current_date.time())
                         - datetime.timedelta(days=7))
        previous_day = (datetime.datetime.combine(self.current_date.date(), self.current_date.time())
                        - datetime.timedelta(hours=24))
        self.r.delete('day~' + str(previous_week))
        self.r.delete('hour~' + str(previous_day))

    def _get_previous_six_hour(self, point):
        six_hours = list()
        output = list()
        previous_six_hours = (datetime.datetime.combine(self.current_date.date(), self.current_date.time())
                              - datetime.timedelta(hours=6))
        for key in self.r.scan_iter('hour~*'):
            decoded_key = key.decode('utf-8')
            key_value = decoded_key.split('~')[1]
            key_dt = datetime.datetime.strptime(key_value, '%Y-%d-%m %H:%M:%S')
            if key_dt >= previous_six_hours:
                value = self.r.lindex(decoded_key, 0)
                six_hours.append(value.decode('utf-8'))
        for data in six_hours:
            splited = data.split('~')
            coordinates = (float(splited[2]), float(splited[3]))
            # TODO coordinates which is given from UI are string
            if coordinates == point:
                output.append(data)
        return output

    def _get_previous_hour(self):
        output = list()
        previous_hour = (datetime.datetime.combine(self.current_date.date(), self.current_date.time())
                         - datetime.timedelta(hours=1))
        for key in self.r.scan_iter('hour~*'):
            decoded_key = key.decode('utf-8')
            key_value = decoded_key.split('~')[1]
            key_dt = datetime.datetime.strptime(key_value, '%Y-%d-%m %H:%M:%S')
            if key_dt >= previous_hour:
                value = self.r.lindex(decoded_key, 0)
                output.append(value.decode('utf-8'))
        return output

    def delete_all_keys(self):
        for i in self.r.scan_iter():
            self.r.delete(i)
        for i in self.r.scan_iter():
            print(i)

    def _produce_answer(self, entry_data):
        """
        separate consumer topic data and group data
        :return:
        """
        self._separation(entry_data=entry_data)
        self._add()
        #self.r.delete(str(self.current_date))
        self._delete()

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
                '''for i in self.r.scan_iter():
                    print(i)'''
                result = self._get_previous_six_hour(point=(40.2201, -74.0021))
                print('result')
                print(result)

                result2 = self._get_previous_hour()
                print('result')
                print(result2)
                
                self._send_data(data=json.loads(each.value.decode('utf-8')), key=each.key, timestamp_ms=each.timestamp)

        else:
            print("No data in previous phase topic")

        #self.delete_all_keys()

