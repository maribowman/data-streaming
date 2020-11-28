"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

BROKER_URLS = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
SCHEMA_REGISTRY = "http://localhost:8081"

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            # TODO *3
            "client.id": "test_client",
            "acks": "all",
            "enable.idempotence": "true",
            "compression.type": "lz4"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {"bootstrap.servers": BROKER_URLS},
            schema_registry=CachedSchemaRegistryClient(SCHEMA_REGISTRY)
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # TODO: Write code that creates the topic for this producer if it does not already exist on the Kafka Broker.
        client = AdminClient({"bootstrap.servers": BROKER_URLS})
        futures = client.create_topics([NewTopic(topic=self.topic_name,
                                                 num_partitions=self.num_partitions,
                                                 replication_factor=self.num_replicas,
                                                 config={
                                                     "cleanup.policy": "compact",
                                                     "compression.type": "lz4"
                                                 })])
        for _, future in futures.items():
            try:
                future.result()
                print(f"topic '{self.topic_name}' created!")
            except Exception as e:
                logger.warning(f"creation of '{self.topic_name}' failed with:\n{e}")
                logger.info("topic creation kafka integration incomplete - skipping")
                pass

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # TODO: Write cleanup code for the Producer here
        client = AdminClient({"bootstrap.servers": BROKER_URLS})
        futures = client.delete_topics([Producer.existing_topics])
        for _, future in futures.items():
            try:
                future.result()
                print(f"topic '{self.topic_name}' successfully deleted!")
            except Exception as e:
                logger.warning(f"deletion of '{self.topic_name}' failed with:\n{e}")
                logger.info("producer close incomplete - skipping")
                pass

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
