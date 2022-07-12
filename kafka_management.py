from kafka.admin import KafkaAdminClient, NewTopic
import time
import threading
import findspark
findspark.init()
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

from settings import BOOTSTRAP_SERVERS as setting_bootstrap_server
from blocks.file_stream import FileStreamBlock
from blocks.elastic_analyse import ElasticAnalyseBlock
from blocks.cassandra_analyse import CassandraAnalyseBlock


class KafkaManagement:
    TOPICS = ['FileDataTopic', 'ClusterTopic', 'ElasticTopic', 'CassandraTopic', ]
    TOPIC_PARTITION = 1
    TOPIC_REPLICATION = 1
    BOOTSTRAP_SERVERS = setting_bootstrap_server
    SPARK_SESSION = SparkSession.builder.config("spark.driver.memory", "2g").appName('taxi').getOrCreate()
    SPARK_SESSION.conf.set("spark.sql.shuffle.partitions", 5)

    def __init__(self):
        """
        initial function
        """
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.BOOTSTRAP_SERVERS)
        self._delete_topics()
        self._create_topics()
        self.threads = dict()

    def _create_topics(self):
        """
        create list of topics
        :return: result messages (the request is successfully done or not)
        """
        topic_list = list()
        for topic in self.TOPICS:
            topic_list.append(NewTopic(name=topic,
                                       num_partitions=self.TOPIC_PARTITION,
                                       replication_factor=self.TOPIC_REPLICATION))
        try_count = 0
        while True:
            try_count += 1
            print(f"try create topics: {try_count}")
            try:
                result_messages = self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
                return result_messages
            except Exception as e:
                # suppose exception for existed topic
                print(e)
                time.sleep(1)

    def _delete_topics(self):
        """
        delete list of topics
        :return: result messages (the request is successfully done or not)
        """
        try:
            result_messages = self.admin_client.delete_topics(self.TOPICS)
            return result_messages
        except Exception as e:
            # suppose exception for not existed topic
            print(e)

    def _make_start_block_thread(self, block, key):
        """
        create a thread add  run and adding to self.theads
        :param block: a object with type of BaseBlock
        :return:
        """
        thread = threading.Thread(target=block.run)
        thread.start()
        self.threads[key] = thread

    def run(self):
        """
        make blocks and run it on threads
        :return:
        """
        # blocks
        file_streamer = FileStreamBlock(bootstrap_servers=self.BOOTSTRAP_SERVERS, producer_topic=self.TOPICS[0])
        self._make_start_block_thread(block=file_streamer, key='file_streamer')
        
        # elasticsearch block
        elastic_analyser = ElasticAnalyseBlock(consumer_topic=self.TOPICS[0], bootstrap_servers=self.BOOTSTRAP_SERVERS,
                                               producer_topic=self.TOPICS[2])
        # TODO consumer_topic=self.TOPICS[1]
        self._make_start_block_thread(block=elastic_analyser, key='elastic_analyser')
        
        # cassandra block
        """# create cluster
        self.cluster = Cluster()
        # create session
        self.session = self.cluster.connect()"""
        cassandra_analyse = CassandraAnalyseBlock(consumer_topic=self.TOPICS[0],
                                                  bootstrap_servers=self.BOOTSTRAP_SERVERS,
                                                  producer_topic=self.TOPICS[3])
        # TODO consumer_topic=self.TOPICS[2]
        self._make_start_block_thread(block=cassandra_analyse, key='cassandra_analyse')

        # wait the all threads run
        while True:
            threads = list(self.threads.values())
            have_alive = False
            for thread in threads:
                if thread.is_alive():
                    have_alive = True

            if not have_alive:
                break

