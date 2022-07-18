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
        # first stream for base of interval separation
        self.origin = None
        self.first = 0
        self.week_keys = list()
        self.week_values = {'date_time': list(), 'lat': list(), 'lon': list(), 'base': list(), 'cluster_number': list()}

        self.midday_keys = list()
        self.midday_values = {'date_time': list(), 'lat': list(), 'lon': list(), 'base': list(),
                              'cluster_number': list()}

        self.month_keys = list()
        self.month_values = {'date_time': list(), 'lat': list(), 'lon': list(), 'base': list(),
                             'cluster_number': list()}

        self.current_uuid = None  # str
        self.current_date = None  # date
        self.current_time = None  # time
        self.current_lat = 0.0
        self.current_lon = 0.0
        self.current_base = None  # str
        self.current_cluster_number = 0

        # should be moved to kafka_management
        # create cluster
        self.cluster = Cluster()
        # create session
        self.session = self.cluster.connect()
        # to automatically cast "insert" statements in the right way for Cassandra
        self.session.encoder.mapping[tuple] = self.session.encoder.cql_encode_tuple

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
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.week_table (week tuple<text,text> PRIMARY KEY,
            date_time list<text>, lat list<float>, lon list<float>, base list<text>, cluster_number list<int>)
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.start_coordinates_table (start tuple<float,float>
            PRIMARY KEY, date_time list<text>, base list<text>, cluster_number list<int>)
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.uuid_table (uuid text PRIMARY KEY, date_time text,
            lat float, lon float, base text, cluster_number int)
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.midday_table (hour tuple<text,text> PRIMARY KEY,
            date_time list<text>, lat list<float>, lon list<float>, base list<text>, cluster_number list<int>)
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.month_table (month tuple<text,text> PRIMARY KEY,
            date_time list<text>, lat list<float>, lon list<float>, base list<text>, cluster_number list<int>)
            """
        )

    def _separation(self, entry_data):
        # convert class byte to dictionary
        consumer_value = json.loads(entry_data.value.decode('utf-8'))
        self.date_time = datetime.datetime.strptime(consumer_value['Date/Time'], self.DATETIME_FORMAT)

        self.current_uuid = entry_data.key.decode("utf-8")
        self.current_date = self.date_time.date()
        self.current_time = self.date_time.time()
        self.current_lat = float(consumer_value['Lat'])
        self.current_lon = float(consumer_value['Lon'])
        self.current_base = consumer_value['Base']
        self.current_cluster_number = 0
        # TODO:self.cluster_numbers.append(int(consumer_value['Cluster_number']))

    def _insert_week_table(self):
        self.session.execute(
            f"""
            INSERT INTO {self.KEYSPACE_NAME}.week_table (week, date_time, lat, lon, base, cluster_number)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ((str(self.week_keys[0]), str(self.week_keys[1])), self.week_values['date_time'],
             self.week_values['lat'], self.week_values['lon'], self.week_values['base'],
             self.week_values['cluster_number'])
        )

    def _insert_midday_table(self):
        self.session.execute(
            f"""
            INSERT INTO {self.KEYSPACE_NAME}.midday_table (hour, date_time, lat, lon, base, cluster_number)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ((str(self.midday_keys[0]), str(self.midday_keys[1])), self.midday_values['date_time'],
             self.midday_values['lat'], self.midday_values['lon'], self.midday_values['base'],
             self.midday_values['cluster_number'])
        )

    def _insert_month_table(self):
        self.session.execute(
            f"""
            INSERT INTO {self.KEYSPACE_NAME}.month_table (month, date_time, lat, lon, base, cluster_number)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ((str(self.month_keys[0]), str(self.month_keys[1])), self.month_values['date_time'],
             self.month_values['lat'], self.month_values['lon'], self.month_values['base'],
             self.month_values['cluster_number'])
        )

    def _insert_start_coordinates(self):
        # check existence of received coordinate
        existence = self.session.execute(
            f"""
            select * from {self.KEYSPACE_NAME}.start_coordinates_table 
            where start = (%s, %s)
            """,
            (self.current_lat, self.current_lon)
        )

        # update correspond record if exists
        if existence:
            dt = [i for i in existence[0].date_time]
            dt.append(str(self.date_time))
            bases = [i for i in existence[0].base]
            bases.append(self.current_base)
            cl_nums = [i for i in existence[0].cluster_number]
            cl_nums.append(self.current_cluster_number)

            self.session.execute(
                f"""
                UPDATE {self.KEYSPACE_NAME}.start_coordinates_table
                SET date_time = %s, base = %s, cluster_number = %s
                WHERE start = (%s, %s)
                """,
                (dt, bases, cl_nums, self.current_lat, self.current_lon)
            )

        else:
            # insert in table if not exists
            self.session.execute(
                f"""
                INSERT INTO {self.KEYSPACE_NAME}.start_coordinates_table (start, date_time, base, cluster_number)
                VALUES (%s, %s, %s, %s)
                """,
                ((self.current_lat, self.current_lon), [str(self.date_time)], [self.current_base],
                 [self.current_cluster_number])
            )

    def _insert_uuid(self):
        self.session.execute(
            f"""
            INSERT INTO {self.KEYSPACE_NAME}.uuid_table (uuid, date_time, lat, lon, base, cluster_number)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (self.current_uuid, str(self.date_time), self.current_lat, self.current_lon, self.current_base,
             self.current_cluster_number)
        )

    def _check_intervals(self):
        # check weekly key
        if self.first == 0:
            next_week = self.current_date + datetime.timedelta(days=7)
            self.week_keys = [self.current_date, next_week]
        else:
            while self.current_date >= self.week_keys[1]:
                if self.week_values['date_time']:
                    self._insert_week_table()
                next_week = self.week_keys[1] + datetime.timedelta(days=7)
                self.week_keys = [self.week_keys[1], next_week]
                for v in self.week_values.values():
                    v.clear()

        self.week_values['date_time'].append(str(self.date_time))
        self.week_values['lat'].append(self.current_lat)
        self.week_values['lon'].append(self.current_lon)
        self.week_values['base'].append(self.current_base)
        self.week_values['cluster_number'].append(self.current_cluster_number)

        # check midday key
        if self.first == 0:
            next_midday = (datetime.datetime.combine(self.date_time.date(), self.date_time.time())
                           + datetime.timedelta(hours=12))
            self.midday_keys = [self.date_time, next_midday]
        else:
            while self.date_time >= self.midday_keys[1]:
                if self.midday_values['date_time']:
                    self._insert_midday_table()
                next_midday = (datetime.datetime.combine(self.midday_keys[1].date(), self.midday_keys[1].time())
                               + datetime.timedelta(hours=12))
                self.midday_keys = [self.midday_keys[1], next_midday]
                for v in self.midday_values.values():
                    v.clear()

        self.midday_values['date_time'].append(str(self.date_time))
        self.midday_values['lat'].append(self.current_lat)
        self.midday_values['lon'].append(self.current_lon)
        self.midday_values['base'].append(self.current_base)
        self.midday_values['cluster_number'].append(self.current_cluster_number)

        # check monthly key
        if self.first == 0:
            next_month = self.current_date + datetime.timedelta(days=30)
            self.month_keys = [self.current_date, next_month]
        else:
            while self.current_date >= self.month_keys[1]:
                if self.month_values['date_time']:
                    self._insert_month_table()
                next_month = self.month_keys[1] + datetime.timedelta(days=30)
                self.month_keys = [self.month_keys[1], next_month]
                for v in self.month_values.values():
                    v.clear()

        self.month_values['date_time'].append(str(self.date_time))
        self.month_values['lat'].append(self.current_lat)
        self.month_values['lon'].append(self.current_lon)
        self.month_values['base'].append(self.current_base)
        self.month_values['cluster_number'].append(self.current_cluster_number)

        # check same_coordinates and uuid
        self._insert_start_coordinates()
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
                if self.first == 0:
                    self.first += 1
        else:
            print("No data in previous phase topic")

        # close connection
        self.session.shutdown()
        self.cluster.shutdown()


