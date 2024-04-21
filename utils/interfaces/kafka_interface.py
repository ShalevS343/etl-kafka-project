from confluent_kafka import Producer, Consumer, KafkaException

import json
import logging
from typing import Callable, Dict

from utils.data_structures.movie import Movie
from utils.config import Config
from utils.singleton import Singleton

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaInterface(Singleton):
    def __init__(self, timeout_interval: int = 10):
        self._consumer: Consumer = Consumer(Config.CONSUMER_CONFIG)
        self._producer: Producer = Producer(Config.PRODUCER_CONFIG)
        self._timeout_interval = timeout_interval
        self._topics = Config.KAFKA_TOPICS

    def update_timeout_interval(self, interval: int) -> None:
        self._timeout_interval = interval

    def produce_to_topic(self, topic: str, data: Dict[str, Movie]) -> None:
        """
        Produce JSON message to the specified topic.

        Parameters:
            topic (str): The topic to which the message will be produced.
            data (Dict[str, Movie]): A dictionary of movies to produce to the topic.

        Returns:
            None
        """

        try:
            # Produce JSON message to the specified topic
            for title, information in data.items():
                movie_json = json.dumps(information, cls=Movie.MovieEncoder)
                self._producer.produce(
                    topic, key=title, value=movie_json)
                self._producer.flush()
        except KafkaException as e:
            logger.exception(f"Error producing to {topic}: {e}")
        

    def subscribe_to_topics(self, callback: Callable) -> None:
        """
        Consume data from the specified topics and pass it to the callback function.

        Parameters:
            topics (List[str]): The list of topics to consume from.
            callback (Callable): The function to call when a message is received.

        Returns:
            None
        """
        
        self._consumer.subscribe(self._topics)

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
            logger.error(f"Error consuming from {msg.topic()}: {e}")
        except KeyboardInterrupt:
            logger.info("Sending Messages To Callback Stopped")
        finally:
            logger.info("Closing Consumer")
            self._consumer.close()
            
    def clean_buffer(self) -> None:
        self._consumer.subscribe(self._topics)

        max_messages = Config.MAX_MESSAGES
        
        try:
            while max_messages > 0:
                msg = self._consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                # Process the consumed message
                data = json.loads(msg.value())
                logger.info(f"Flushed movie\t{data['imdb_id']}")
                self._consumer.commit()
                
                max_messages -= 1
        except KafkaException as e:
            logger.error(f"Error consuming from {msg.topic()}: {e}")
        except KeyboardInterrupt:
            logger.info("Clearing Stopped")
        finally:
            logger.info("Closing Consumer")
            self._consumer.close()
