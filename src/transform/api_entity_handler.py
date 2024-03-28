from utils.interfaces.kafka_interface import kafka_interface
from src.transform.omdb_entity_handler import OmdbEntityHandler
from src.transform.tmdb_entity_handler import TmdbEntityHandler
from src.transform.entity_handler import entity_handler
from src.transform.genre_handler import genre_handler
from src.transform.actor_handler import actor_handler
from src.transform.bafta_handler import bafta_handler
from src.transform.oscar_handler import oscar_handler


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
        if topic is not None:
            self._process_message(topic, data)
        else:
            raise ValueError('No topic was given in the message!')
        
    def _process_message(self, topic, data):
        """
        Processes each Kafka message by formatting the data and updating the DataFrame.

        Parameters:
        - data (dict): Raw data received from Kafka message.
        """
        if topic == 'nosaqtgg-tmdb-api':
            data = self._tmdb_handler.format_data(data)
            data['awards'] = self._gather_awards(data.get('movie_name'))
            
        else:
            data = self._omdb_handler.format_data(data)
        imdb_id = str(data.get('imdb_id'))
        data['genres'] = genre_handler.get_genre(imdb_id)
        data['lead_actors'] = actor_handler.get_actor(imdb_id)
        
        entity_handler.edit_row(imdb_id, data)
        
        # TODO: Send data to Redis database
    
    def _gather_awards(self, film):
        awards = None
        oscar_awards = oscar_handler.get_awards(film) or []
        bafta_awards = bafta_handler.get_awards(film) or []
        
        if len(oscar_awards) or len(bafta_awards):
            awards = oscar_awards
            awards.extend(bafta_awards)
        return awards
