from kafka.admin import KafkaAdminClient, NewTopic

from settings import BOOTSTRAP_SERVERS as setting_booststrap_server
import base_classes


class KafkaManagement:
    TOPICS = ['FileDataTopic', 'ClusterTopic', ]
    TOPIC_PARTITION = 1
    TOPIC_REPLICATION = 1
    BOOTSTRAP_SERVERS = setting_booststrap_server

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
        try:
            result_messages = self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
            return result_messages
        except Exception as e:
            # suppose exception for existed topic
            print(e)

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

    def run(self):
        pass
