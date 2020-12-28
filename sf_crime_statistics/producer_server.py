import json
import time
from os import path
from zipfile import ZipFile

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from kafka import KafkaProducer


def dict_to_binary(json_dict):
    return json.dumps(json_dict).encode('utf-8')


class ProducerServer(KafkaProducer):
    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        if not self.topic_exists():
            self.create_topic()

    def topic_exists(self):
        try:
            client = AdminClient({"bootstrap.servers": self.config['bootstrap_servers']})
        except Exception as e:
            print(f"failed to connect to kafka:\n{e}")
        cluster_metadata = client.list_topics(timeout=5)
        for topic in cluster_metadata.topics:
            if self.topic == topic:
                print(f'topic "{topic}" already exists - skipping!')
                return True
        return False

    def create_topic(self):
        print(f"kafka topic '{self.topic}' does not exist. starting topic creation!")
        client = AdminClient({"bootstrap.servers": self.config['bootstrap_servers']})
        futures = client.create_topics([NewTopic(topic=self.topic,
                                                 num_partitions=1,
                                                 replication_factor=1,
                                                 )]
                                       )
        for topic, future in futures.items():
            try:
                future.result()
                print(f"topic '{topic}' created!")
            except Exception as e:
                print(f"creation of '{self.topic}' failed with:\n{e}")
                pass

    def generate_data(self):
        if not path.exists(f'{self.input_file}.json'):
            print("json file does not exist. extracting zip file.")
            with ZipFile(f'{self.input_file}.zip', 'r') as zip_file:
                zip_file.extractall('./')
        with open(f'{self.input_file}.json') as file:
            print("starting to send data to kafka.")
            for line in json.load(file):
                message = dict_to_binary(line)
                self.send(topic=self.topic, value=message)
                time.sleep(1)
