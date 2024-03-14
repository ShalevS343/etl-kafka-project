import json

from confluent_kafka import Producer, Consumer, KafkaException

from utils.config import Config
from utils.logging import logger

class KafkaInterface:
    def __init__(self):     
        self._consumer = Consumer(Config.CONSUMER_CONFIG)
        self._producer = Producer(Config.PRODUCER_CONFIG)

    def produce_to_topic(self, topic, data):
        try:
            # Produce JSON message to the specified topic
            for title, information in data.items():
                self._producer.produce(topic, key=None, value=json.dumps({title: information}))
                self._producer.flush()
        except KafkaException as e:
            logger.exception(f"Error producing to {topic}: {e}")

    def consume_from_topic(self, topics, callback):
        self._consumer.subscribe(topics)

        try:
            while True:
                msg = self._consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                # Process the consumed message
                data = json.loads(msg.value())
                callback(msg.topic(), data)
                
                self._consumer.commit()
        except KafkaException as e:
            logger.exception(f"Error consuming from {msg.topic()}: {e}")
        finally:
            self._consumer.close()


# Singleton instance of KafkaInterface for easy access
kafka_interface = KafkaInterface()