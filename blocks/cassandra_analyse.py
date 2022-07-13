from abc import ABC
import json
import datetime
from cassandra.cluster import Cluster

from blocks.base_classes import BaseBlock, BlockType
from settings import KEYSPACE_NAME as setting_keyspace_name


class CassandraAnalyseBlock(BaseBlock, ABC):
    KEYSPACE_NAME = setting_keyspace_name
    DATETIME_FORMAT = "%m/%d/%Y %H:%M:%S"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)
        self.week_separation = {'uuids': list(), 'dates': list(), 'times': list(), 'coordinates': list(tuple()),
                                'bases': list(), 'cluster_numbers': list()}
        self.midday_separation = {'uuids': list(), 'dates': list(), 'times': list(), 'coordinates': list(tuple()),
                                  'bases': list(), 'cluster_numbers': list()}
        self.month_separation = {'uuids': list(), 'dates': list(), 'times': list(), 'coordinates': list(tuple()),
                                 'bases': list(), 'cluster_numbers': list()}
        self.same_start = dict()
        self.unique_uuid = dict()

        self.next_week = list()
        self.next_midday = list()
        self.next_month = list()

        # should be moved to kafka_management
        # create cluster
        self.cluster = Cluster()
        # create session
        self.session = self.cluster.connect()
        # to automatically cast "insert" statements in the right way for Cassandra
        self.session.encoder.mapping[tuple] = self.session.encoder.cql_encode_tuple

    def _separation(self, entry_data):
        # convert class byte to dictionary
        consumer_value = json.loads(entry_data.value.decode('utf-8'))

        # separate uuid
        self.week_separation['uuids'].append(entry_data.key.decode("utf-8"))
        self.midday_separation['uuids'].append(entry_data.key.decode("utf-8"))
        self.month_separation['uuids'].append(entry_data.key.decode("utf-8"))
        self.same_start['uuids'] = entry_data.key.decode("utf-8")
        self.unique_uuid['uuids'] = entry_data.key.decode("utf-8")

        # separate date and time
        date_time = datetime.datetime.strptime(consumer_value['Date/Time'], self.DATETIME_FORMAT)
        # date
        self.week_separation['dates'].append(date_time.date())
        self.midday_separation['dates'].append(date_time.date())
        self.month_separation['dates'].append(date_time.date())
        self.same_start['dates'] = date_time.date()
        self.unique_uuid['dates'] = date_time.date()
        # time
        self.week_separation['times'].append(date_time.time())
        self.midday_separation['times'].append(date_time.time())
        self.month_separation['times'].append(datetime.time())
        self.same_start['times'] = date_time.time()
        self.unique_uuid['times'] = date_time.time()

        # separate coordinates
        self.week_separation['coordinates'].append((float(consumer_value['Lat']), float(consumer_value['Lon'])))
        self.midday_separation['coordinates'].append((float(consumer_value['Lat']), float(consumer_value['Lon'])))
        self.month_separation['coordinates'].append((float(consumer_value['Lat']), float(consumer_value['Lon'])))
        self.same_start['coordinates'] = (float(consumer_value['Lat']), float(consumer_value['Lon']))
        self.unique_uuid['coordinates'] = (float(consumer_value['Lat']), float(consumer_value['Lon']))

        # separate base
        self.week_separation['bases'].append(consumer_value['Base'])
        self.midday_separation['bases'].append(consumer_value['Base'])
        self.month_separation['bases'].append(consumer_value['Base'])
        self.same_start['bases'] = consumer_value['Base']
        self.unique_uuid['bases'] = consumer_value['Base']

        # separate cluster_number
        self.week_separation['cluster_numbers'].append(0)
        self.midday_separation['cluster_numbers'].append(0)
        self.month_separation['cluster_numbers'].append(0)
        self.same_start['cluster_numbers'] = 0
        self.unique_uuid['cluster_numbers'] = 0
        # TODO:self.cluster_numbers.append(int(consumer_value['Cluster_number']))

    def _create_tables(self):
        """
        create tables and keyspace
        :return:
        """
        # create keyspace
        self.session.execute(f"""create  keyspace IF NOT EXISTS {self.KEYSPACE_NAME} """ +
                             """with replication={'class': 'SimpleStrategy', 'replication_factor': 3}""")

        # create tables
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.week_table (week tuple< tuple<date, time>, tuple<date, time> >
            PRIMARY KEY, Lat list<float>, Lon list<float>, Base list<text>, Cluster_number list<int>,
            str_week tuple<text, text>)
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.start_coordinates_table (start tuple<float, float>
            PRIMARY KEY, Date list<date>, Time list<time>, Base list<text>, Cluster_number list<int>)
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.uuid_table (uuid text PRIMARY KEY, Date date, Time time,
            Lat float, Lon float, Base text, Cluster_number int)
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.hours_table (hour tuple< tuple<date, time>, tuple<date, time>>
            PRIMARY KEY, Lat list<float>, Lon list<float>, Base list<text>, Cluster_number list<int>,
            str_hour tuple<text, text>)
            """
        )
        
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.month_table (month tuple<tuple<date, time>, tuple<date,time>>
            PRIMARY KEY, Lat list<float>, Lon list<float>, Base list<text>, Cluster_number list<int>,
            str_month tuple<text, text>)
            """
        )

    def _insert_same_coordinates(self):
        # check existence of received coordinate
        existence = self.session.execute(
            f"""
                select * from {self.KEYSPACE_NAME}.start_coordinates_table 
                where start = {self.same_start['coordinates'][-1]}
                """
        )

        # update correspond record if exists
        if existence:
            self.session.execute(
                f"""
                UPDATE {self.KEYSPACE_NAME}.start_coordinates_table
                SET Date = %s, Time = %s, Base = %s, Cluster_number = %s
                """,
                ([existence[0].date]+[self.same_start['dates']], [existence[0].time]+[self.same_start['times']],
                 [existence[0].base]+[self.same_start['bases']],
                 [existence[0].cluster_number]+[self.same_start['cluster_numbers']])
            )

        # insert in table if not exists
        self.session.execute(
            f"""
            INSERT INTO {self.KEYSPACE_NAME}.start_coordinates_table (start, Date, Time, Base, Cluster_number)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (self.same_start['coordinates'], [self.same_start['dates']], [self.same_start['times']],
             [self.same_start['bases']], [self.same_start['cluster_numbers']])
        )

        # delete inserted data
        self.same_start.clear()

    def _insert_uuid(self):
        self.session.execute(
            f"""
            INSERT INTO {self.KEYSPACE_NAME}.uuid_table (uuid, Date, Time, Lat, Lon, Base, Cluster_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (self.unique_uuid['uuids'], self.unique_uuid['dates'], self.unique_uuid['times'],
             self.unique_uuid['coordinates'][0], self.unique_uuid['coordinates'][1], self.unique_uuid['bases'],
             self.unique_uuid['cluster_numbers'])
        )

    def _check_intervals(self):
        # check weekly key
        if self.week_separation['dates'][-1] in self.next_week:
            # insert in correspond table
            self.session.execute(
                f"""
                INSERT INTO {self.KEYSPACE_NAME}.week_table (week, Lat, Lon, Base, Cluster_number, str_week)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (((self.week_separation['dates'][0], self.week_separation['times'][0]),
                  (self.week_separation['dates'][-1], self.week_separation['times'][-1])),
                 [i[0] for i in self.midday_separation['coordinates']],
                 [i[1] for i in self.midday_separation['coordinates']],
                 self.week_separation['bases'], self.week_separation['cluster_numbers'],
                 (self.week_separation['dates'][0].toString()+self.week_separation['times'][0].toString(),
                  self.week_separation['dates'][-1].toString()+self.week_separation['times'][-1].toString()))
            )

            # delete inserted data from lists
            current = self.week_separation['dates'][0]
            while self.week_separation['dates'][0] == current:
                for v in self.week_separation.values():
                    v.pop(0)
            self.next_week.pop(0)

        # check midday key
        elif self.midday_separation['times'][-1] in self.next_midday:
            # insert in correspond table
            self.session.execute(
                f"""
                INSERT INTO {self.KEYSPACE_NAME}.hours_table (hour, Lat, Lon, Base, Cluster_number, str_hour)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (((self.midday_separation['dates'][0], self.midday_separation['times'][0]),
                  (self.midday_separation['dates'][-1], self.midday_separation['times'][-1])),
                 [i[0] for i in self.midday_separation['coordinates']],
                 [i[1] for i in self.midday_separation['coordinates']],
                 self.midday_separation['bases'], self.midday_separation['cluster_numbers'],
                 (self.midday_separation['dates'][0].toString()+self.midday_separation['times'][0].toString(),
                  self.midday_separation['dates'][-1].toString()+self.midday_separation['times'][-1].toString()))
            )

            # delete inserted data from lists
            current = self.midday_separation['times'][0]
            while self.midday_separation['dates'][0] == current:
                for v in self.midday_separation.values():
                    v.pop(0)
            self.next_midday.pop(0)
            
        # check monthly key
        elif self.month_separation['dates'][-1] in self.next_month:
            # insert in correspond table
            self.session.execute(
                f"""
                INSERT INTO {self.KEYSPACE_NAME}.month_table (month, Lat, Lon, Base, Cluster_number, str_month)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (((self.month_separation['dates'][0], self.month_separation['times'][0]),
                  (self.month_separation['dates'][-1], self.month_separation['times'][-1])),
                 [i[0] for i in self.month_separation['coordinates']],
                 [i[1] for i in self.month_separation['coordinates']],
                 self.month_separation['bases'], self.month_separation['cluster_numbers'],
                 (self.month_separation['dates'][0].toString()+self.month_separation['times'][0].toString(),
                  self.month_separation['dates'][-1].toString()+self.month_separation['times'][-1].toString()))
            )

            # delete inserted data from lists
            current = self.month_separation['dates'][0]
            while self.month_separation['dates'][0] == current:
                for v in self.month_separation.values():
                    v.pop(0)
            self.next_month.pop(0)

        else:
            next_w = self.week_separation['dates'][-1] + datetime.timedelta(days=7)
            if next_w not in self.next_week:
                self.next_week.append(next_w)
            
            next_t = (datetime.datetime.combine(datetime.date(1, 1, 1), self.midday_separation['times'][-1]) +
                      datetime.timedelta(hours=12)).time()
            if next_t not in self.next_midday:
                self.next_midday.append(next_t)
                
            next_m = self.month_separation['dates'][-1] + datetime.timedelta(days=30)
            if next_m not in self.next_month:
                self.next_month.append(next_m)

        self._insert_same_coordinates()
        self._insert_uuid()

    def _produce_answer(self, entry_data):
        """
        separate consumer topic data and group data
        :return:
        """
        self._separation(entry_data=entry_data)
        self._check_intervals()

    def _normal_run(self):
        """
        run method for normal type
            create normal
        :return:
        """
        self._normal_setup()
        self._create_tables()

        if self.consumer:
            for each in self.consumer:
                self._produce_answer(each)
        else:
            print("No data in previous phase topic")

        # close connection
        self.session.shutdown()
        self.cluster.shutdown()
       
