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
        self.week_keys = list(list())
        self.midday_keys = list(list())
        self.month_keys = list(list())

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
        self.session.execute(f"""DROP KEYSPACE IF EXISTS {self.KEYSPACE_NAME.lower()}""")
        self.session.execute(f"""create  keyspace IF NOT EXISTS {self.KEYSPACE_NAME} """ +
                             """with replication={'class': 'SimpleStrategy', 'replication_factor': 3}""")

        # create tables
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.week_table (week tuple<text,text> PRIMARY KEY,
            lat list<float>, lon list<float>, base list<text>, cluster_number list<int>)
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
            lat list<float>, lon list<float>, base list<text>, cluster_number list<int>)
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.KEYSPACE_NAME}.month_table (month tuple<text,text> PRIMARY KEY,
            lat list<float>, lon list<float>, base list<text>, cluster_number list<int>)
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
            INSERT INTO {self.KEYSPACE_NAME}.week_table (week, lat, lon, base, cluster_number)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ((str(self.week_keys[-1][0]), str(self.week_keys[-1][1])), [0.0], [0.0], [''], [0])
        )

    def _update_week_table(self):
        # check whether data entered or not
        empty = self.session.execute(
            f"""
            select * from {self.KEYSPACE_NAME}.week_table 
            where week = (%s, %s)
            """,
            (str(self.week_keys[0][0]), str(self.week_keys[0][1]))
        )

        # insert current stream to correspond key if no data have entered before
        # else add current stream to correspond record
        if empty[0].base[0] == '':
            latitude = list()
            longitude = list()
            base = list()
            cluster = list()
        else:
            latitude = [i for i in empty[0].lat]
            longitude = [i for i in empty[0].lon]
            base = [i for i in empty[0].base]
            cluster = [i for i in empty[0].cluster_number]

        latitude.append(self.current_lat)
        longitude.append(self.current_lon)
        base.append(self.current_base)
        cluster.append(self.current_cluster_number)

        # update correspond record
        self.session.execute(
            f"""
            UPDATE {self.KEYSPACE_NAME}.week_table
            SET lat = %s, lon = %s, base = %s, cluster_number = %s
            WHERE week = (%s, %s)
            """,
            (latitude, longitude, base, cluster, str(self.week_keys[0][0]), str(self.week_keys[0][1]))
        )

    def _insert_midday_table(self):
        self.session.execute(
            f"""
            INSERT INTO {self.KEYSPACE_NAME}.midday_table (hour, lat, lon, base, cluster_number)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ((str(self.midday_keys[-1][0]), str(self.midday_keys[-1][1])), [0.0], [0.0], [''], [0])
        )

    def _update_midday_table(self):
        # check whether data entered or not
        empty = self.session.execute(
            f"""
            select * from {self.KEYSPACE_NAME}.midday_table 
            where hour = (%s, %s)
            """,
            (str(self.midday_keys[0][0]), str(self.midday_keys[0][1]))
        )

        # insert current stream to correspond key if no data have entered before
        # else add current stream to correspond record
        if empty[0].base[0] == '':
            latitude = list()
            longitude = list()
            base = list()
            cluster = list()
        else:
            latitude = [i for i in empty[0].lat]
            longitude = [i for i in empty[0].lon]
            base = [i for i in empty[0].base]
            cluster = [i for i in empty[0].cluster_number]

        latitude.append(self.current_lat)
        longitude.append(self.current_lon)
        base.append(self.current_base)
        cluster.append(self.current_cluster_number)

        # update correspond record
        self.session.execute(
            f"""
            UPDATE {self.KEYSPACE_NAME}.midday_table
            SET lat = %s, lon = %s, base = %s, cluster_number = %s
            WHERE hour = (%s, %s)
            """,
            (latitude, longitude, base, cluster, str(self.midday_keys[0][0]), str(self.midday_keys[0][1]))
        )

    def _insert_month_table(self):
        self.session.execute(
            f"""
            INSERT INTO {self.KEYSPACE_NAME}.month_table (month, lat, lon, base, cluster_number)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ((str(self.month_keys[-1][0]), str(self.month_keys[-1][1])), [0.0], [0.0], [''], [0])
        )

    def _update_month_table(self):
        # check whether data entered or not
        empty = self.session.execute(
            f"""
            select * from {self.KEYSPACE_NAME}.month_table 
            where month = (%s, %s)
            """,
            (str(self.month_keys[0][0]), str(self.month_keys[0][1]))
        )

        # insert current stream to correspond key if no data have entered before
        # else add current stream to correspond record
        if empty[0].base[0] == '':
            latitude = list()
            longitude = list()
            base = list()
            cluster = list()
        else:
            latitude = [i for i in empty[0].lat]
            longitude = [i for i in empty[0].lon]
            base = [i for i in empty[0].base]
            cluster = [i for i in empty[0].cluster_number]

        latitude.append(self.current_lat)
        longitude.append(self.current_lon)
        base.append(self.current_base)
        cluster.append(self.current_cluster_number)

        # update correspond record
        self.session.execute(
            f"""
            UPDATE {self.KEYSPACE_NAME}.month_table
            SET lat = %s, lon = %s, base = %s, cluster_number = %s
            WHERE month = (%s, %s)
            """,
            (latitude, longitude, base, cluster, str(self.month_keys[0][0]), str(self.month_keys[0][1]))
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
            start_date = self.current_date
        else:
            start_date = self.week_keys[-1][1]
        for i in range(3):
            next_week = start_date + datetime.timedelta(days=7)
            self.week_keys.append([start_date, next_week])
            self._insert_week_table()
            start_date = self.week_keys[-1][1]

        if self.current_date >= self.week_keys[0][1]:
            self.week_keys.pop(0)

        self._update_week_table()

        # check midday key
        if self.first == 0:
            start = self.date_time
        else:
            start = self.midday_keys[-1][1]
        for i in range(3):
            next_midday = (datetime.datetime.combine(start.date(), start.time())+datetime.timedelta(hours=12))
            self.midday_keys.append([start, next_midday])
            self._insert_midday_table()
            start = self.midday_keys[-1][1]

        if self.date_time >= self.midday_keys[0][1]:
            self.midday_keys.pop(0)

        self._update_midday_table()

        # check monthly key
        if self.first == 0:
            start_month = self.current_date
        else:
            start_month = self.month_keys[-1][1]
        for i in range(3):
            next_month = start_month + datetime.timedelta(days=30)
            self.month_keys.append([start_month, next_month])
            self._insert_month_table()
            start_month = self.month_keys[-1][1]

        if self.current_date >= self.month_keys[0][1]:
            self.month_keys.pop(0)

        self._update_month_table()

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
                self._send_data(data=json.loads(each.value.decode('utf-8')), key=each.key, timestamp_ms=each.timestamp)
        else:
            print("No data in previous phase topic")

        # close connection
        self.session.shutdown()
        self.cluster.shutdown()
