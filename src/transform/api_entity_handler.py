from utils.interfaces.kafka_interface import kafka_interface
from src.transform.omdb_entity_handler import OmdbEntityHandler
from src.transform.tmdb_entity_handler import TmdbEntityHandler

class ApiEntityHandler():
    def __init__(self):
        """
        Initializes an instance of ApiEntityHandler.

        This class is responsible for handling data received from Kafka topics
        and sending it to the appropriate handler based on the topic.

        """
        self._tmdb_handler = TmdbEntityHandler()
        self._omdb_handler = OmdbEntityHandler()
        
    
    def start_processing(self, kafka_topics):
        """
        Starts consuming data from Kafka topics and processes each message.

        Parameters:
        - kafka_topics (list): List of Kafka topics to consume data from.
        """
        kafka_interface.consume_from_topic(kafka_topics, self.send_to_handler)

    def send_to_handler(self, topic, data):
        """
        Sends the data to the appropriate handler based on the Kafka topic.

        Parameters:
        - topic (str): Kafka topic from which the data is received.
        - data (dict): Raw data received from Kafka message.
        """
        if topic == 'nosaqtgg-tmdb-api':
            self._tmdb_handler.process_message(data=data)
        elif topic == 'nosaqtgg-omdb-api':
            self._omdb_handler.process_message(data=data)
        else:
            raise ValueError('No topic given in the message!')