from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession
import time
import threading
import findspark
findspark.init()

from settings import BOOTSTRAP_SERVERS as setting_bootstrap_server
from blocks.file_stream import FileStreamBlock
from blocks.elastic_analyse import ElasticAnalyseBlock
from blocks.cassandra_analyse import CassandraAnalyseBlock
from blocks.redis_analyse import RedisAnalyseBlock
from blocks.page_rank import PageRankBlock
from blocks.online_cluster import PreTrainedClusteringBlock, OnlineClusteringBlock
from blocks.predictor import CountPredictorBlock


class KafkaManagement:
    TOPICS = ['FileDataTopic', 'ClusterTopic', 'ElasticTopic', 'CassandraTopic', 'RedisTopic',
              'PredictorTopic', 'PageRankTopic']

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
        time.sleep(5)
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
                break

    def _delete_topics(self):
        """
        delete list of topics
        :return: result messages (the request is successfully done or not)
        """
        try:
            result_messages = self.admin_client.delete_topics(self.TOPICS+['__consumer_offsets'])
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

        pre_trained_cluster = PreTrainedClusteringBlock(bootstrap_servers=self.BOOTSTRAP_SERVERS,
                                                        producer_topic=self.TOPICS[1],
                                                        consumer_topic=self.TOPICS[0],
                                                        spark_session=self.SPARK_SESSION)
        self._make_start_block_thread(block=pre_trained_cluster, key='pre_trained_cluster')

        # online_clustering = OnlineClusteringBlock(bootstrap_servers=self.BOOTSTRAP_SERVERS,
        #                                           producer_topic=self.TOPICS[1],
        #                                           consumer_topic=self.TOPICS[0],
        #                                           spark_session=self.SPARK_SESSION)

        # online_clustering.run()
        # redis block
        redis_analyser = RedisAnalyseBlock(consumer_topic=self.TOPICS[0], bootstrap_servers=self.BOOTSTRAP_SERVERS,
                                           producer_topic=self.TOPICS[4])
        # TODO consumer_topic=self.TOPICS[3]
        self._make_start_block_thread(block=redis_analyser, key='redis_analyser')
        # self._make_start_block_thread(block=online_clustering, key='online_clustering')

        page_rank = PageRankBlock(consumer_topic=self.TOPICS[0],
                                  bootstrap_servers=self.BOOTSTRAP_SERVERS,
                                  producer_topic=self.TOPICS[-1],
                                  spark_session=self.SPARK_SESSION)
        self._make_start_block_thread(block=page_rank, key='page_rank')
        predictor_block = CountPredictorBlock(consumer_topic=self.TOPICS[3],
                                              bootstrap_servers=self.BOOTSTRAP_SERVERS,
                                              producer_topic=self.TOPICS[4],
                                              spark_session=self.SPARK_SESSION)
        predictor_block.run()

        # wait the all threads run
        while True:
            threads = list(self.threads.values())
            have_alive = False
            for thread in threads:
                if thread.is_alive():
                    have_alive = True

            if not have_alive:
                break

