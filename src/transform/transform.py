from src.transform.api_entity_handler import ApiEntityHandler
from src.transform.entity_handler import EntityHandler


class Transform:
    def __init__(self, kafka_topics=['nosaqtgg-tmdb-api', 'nosaqtgg-omdb-api']):
        """
        Initializes the Transform class with SparkSession and DataFrame schema.

        Parameters:
        - kafka_topics (list): List of Kafka topics to consume data from.
        """
        self._api_entity_handler = ApiEntityHandler()
        self._kafka_topics = kafka_topics

    def start(self):
        """
        Starts consuming data from Kafka topics and processes each message.
        """
        self._api_entity_handler.start_processing(self._kafka_topics)
