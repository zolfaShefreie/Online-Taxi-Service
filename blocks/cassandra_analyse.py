from abc import ABC
import json
import datetime
from cassandra.cluster import Cluster

from blocks.base_classes import BaseBlock, BlockType


class CassandraAnalyseBlock(BaseBlock, ABC):
    uuids = list()
    dates = list()
    times = list()
    lats = list()
    lons = list()
    bases = list()
    cluster_numbers = list()

    DATETIME_FORMAT = "%m/%d/%Y %H:%M:%S"

    def __init__(self, *args, **kwargs):
        super().__init__(block_type=BlockType.normal, consumer_group_id=None, *args, **kwargs)
        self.keyspace_name = 'myKeyspace'
        # create cluster
        self.cluster = Cluster()
        # create session
        self.session = self.cluster.connect()
        # to automatically cast "insert" statements in the right way for Cassandra
        self.session.encoder.mapping[tuple] = self.session.encoder.cql_encode_tuple

    def _create_tables(self):
        """
        create tables
        :return:
        """
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS myKeyspace.week_key (week tuple< tuple<date, time>, tuple<date, time> >
            PRIMARY KEY, Lat list<float>, Lon list<float>, Base list<text>, Cluster_number list<int>)
            """
        )

        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS myKeyspace.start_coordinates_key (start tuple<float, float> PRIMARY KEY,
            Date list<date>, Time list<time>, Base list<text>, Cluster_number list<int>)
            """
        )

        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS myKeyspace.uuid_key (uuid text PRIMARY KEY, Date date, Time time, Lat float,
            Lon float, Base text, Cluster_number int)
            """
        )

        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS myKeyspace.hours_key (hour tuple< tuple<date, time>, tuple<date, time> >
            PRIMARY KEY, Lat list<float>, Lon list<float>, Base list<text>, Cluster_number list<int>)
            """
        )

    def _insert_table(self, table_type, key, uuids, dates, times, lats, lons, bases, cluster_nums):
        """
        insert data in tables
        :param table_type: which table to insert in
        :param key: table key
        :param uuids:
        :param dates:
        :param times:
        :param lats:
        :param lons:
        :param bases:
        :param cluster_nums:
        :return:
        """
        if table_type == 'week':
            self.session.execute(
                """
                INSERT INTO myKeyspace.week_key (week, Lat, Lon, Base, Cluster_number)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (key, lats, lons, bases, cluster_nums)
            )

        elif table_type == 'start_coordinates':
            self.session.execute(
                """
                INSERT INTO myKeyspace.start_coordinates_key (start, Date, Time, Base, Cluster_number)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (key, dates, times, bases, cluster_nums)
            )

        elif table_type == 'uuid':
            self.session.execute(
                """
                INSERT INTO myKeyspace.uuid_key (uuid, Date, Time, Lat, Lon, Base, Cluster_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (key, dates, times, lats, lons, bases, cluster_nums)
            )

        else:
            self.session.execute(
                """
                INSERT INTO myKeyspace.hours_key (hour, Lat, Lon, Base, Cluster_number)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (key, lats, lons, bases, cluster_nums)
            )

    def _weekly_key(self):
        """
        group data in week intervals
        :return:
        """
        start_date = self.dates[0]
        end_date = self.dates[-1]
        interval = datetime.timedelta(days=7)

        periods = list()
        while start_date < end_date:
            periods.append(start_date)
            start_date += interval

        # insert in correspond table
        for k in range(len(periods)-1):
            start = self.dates.index(periods[k])
            end = self.dates.index(periods[k+1])
            self._insert_table('week', None, None, None,
                               ((self.dates[start], self.times[start]), (self.dates[end], self.times[end])),
                               [self.lats[m] for m in range(start, end+1)], [self.lons[m] for m in range(start, end+1)],
                               [self.bases[m] for m in range(start, end+1)],
                               [self.cluster_numbers[m] for m in range(start, end+1)])

    def _hours_key(self):
        """
        group data in 12 hours intervals
        :return:
        """
        start_time = self.times[0]
        end_time = self.times[-1]
        interval = datetime.timedelta(hours=12)
        periods = list()
        while start_time < end_time:
            periods.append(start_time)
            start_time = (datetime.datetime.combine(datetime.date(1, 1, 1), start_time) + interval).time()
            # combine() function lifts the start_time to a datetime.datetime object, the interval is then added,
            # and the result is dropped back down to a datetime.time object.

        # insert in correspond table
        for k in range(len(periods)-1):
            start = self.dates.index(periods[k])
            end = self.dates.index(periods[k+1])
            self._insert_table('hours', None, None, None,
                               ((self.dates[start], self.times[start]), (self.dates[end], self.times[end])),
                               [self.lats[m] for m in range(start, end+1)], [self.lons[m] for m in range(start, end+1)],
                               [self.bases[m] for m in range(start, end+1)],
                               [self.cluster_numbers[m] for m in range(start, end+1)])

    def _start_coordinates(self):
        """
        group data according to same start coordinations
        :return:
        """
        i = 0
        matches = list()
        current_match = list()
        while i < len(self.lats):
            current_match.clear()
            if i not in matches:
                for index, element in enumerate(self.lats):
                    if element == self.lats[i] and self.lons[index] == self.lons[i]:
                        matches.append(index)
                        current_match.append(index)
            else:
                pass

            self._insert_table('start_coordinates', (self.lats[i], self.lons[i]), None,
                               [self.dates[m] for m in current_match], [self.times[m] for m in current_match], None,
                               None, [self.bases[m] for m in current_match],
                               [self.cluster_numbers[m] for m in current_match])

            while i+1 < len(self.lats) and self.lats[i+1] == self.lats[i]:
                i += 1

            matches.append(-1)
            i += 1

    def _uuid_key(self):
        """
        group data according to uuids
        :return:
        """
        for i in range(len(self.uuids)):
            self._insert_table('uuid', self.uuids[i], None, self.dates[i], self.times[i], self.lats[i], self.lons[i],
                               self.bases[i], self.cluster_numbers[i])

    def _produce_answer(self, entry_data):
        """
        separate consumer topic data
        :return:
        """
        # convert class byte to dictionary
        consumer_value = json.loads(entry_data.value.decode('utf-8'))

        self.uuids.append(entry_data.key.decode("utf-8"))
        date_time = datetime.datetime.strptime(consumer_value['Date/Time'], self.DATETIME_FORMAT)
        self.dates.append(date_time.date())
        self.times.append(date_time.time())
        self.lats.append(float(consumer_value['Lat']))
        self.lons.append(float(consumer_value['Lon']))
        self.bases.append(consumer_value['Base'])
        self.cluster_numbers.append(0)
        # TODO:self.cluster_numbers.append(int(consumer_value['Cluster_number']))

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

        self._weekly_key()
        self._hours_key()
        self._start_coordinates()
        self._uuid_key()

        # close connection
        self.session.shutdown()
        self.cluster.shutdown()
       
