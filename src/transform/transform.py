from src.transform.api_entity_handler import ApiEntityHandler  # Adjust the import based on your project structure

class Transform:
    def __init__(self, kafka_topics=['nosaqtgg-tmdb-api', 'nosaqtgg-omdb-api']):
        """
        Initializes the Transform class with SparkSession and DataFrame schema.

        Parameters:
        - kafka_topics (list): List of Kafka topics to consume data from.
        """

        # Initialize the ApiEntityHandler class
        self._api_entity_handler = ApiEntityHandler()

        # Kafka topics
        self._kafka_topics = kafka_topics

    def start(self):
        """
        Starts consuming data from Kafka topics and processes each message.
        """
        # Start processing messages using ApiEntityHandler
        self._api_entity_handler.start_processing(self._kafka_topics)



