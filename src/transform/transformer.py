from typing import List
from src.transform.api_entity_handler import ApiEntityHandler


class Transformer:
    def __init__(self, kafka_topics: List[str]):
        """
        Initializes an instance of Transformer.

        Parameters:
            kafka_topics (List[str]): List of Kafka topics to consume data from.
        """
        self._api_entity_handler = ApiEntityHandler()
        self._kafka_topics = kafka_topics

    def transform(self):
        """
        Starts consuming data from Kafka topics and processes each message.
        """
        self._api_entity_handler.start_processing(self._kafka_topics)
