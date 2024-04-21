from typing import List

from utils.data_structures.movie import Movie
from utils.exceptions import NoTopicGivenError
from utils.interfaces.pyspark_interface import PysparkInterface
from utils.interfaces.kafka_interface import KafkaInterface
from src.transform.genre_handler import GenreHandler
from src.transform.actor_handler import ActorHandler
from src.transform.bafta_handler import BaftaHandler
from src.transform.oscar_handler import OscarHandler

class ApiEntityHandler():
    def __init__(self):
        """
        Initializes an instance of ApiEntityHandler.

        This class is responsible for handling data received from Kafka topics
        and sending it to the appropriate handler based on the topic.

        """
        self._kafka_interface = KafkaInterface()
        self._pyspark_interface = PysparkInterface()
        
        self._bafta_handler = BaftaHandler()
        self._oscar_handler = OscarHandler()
        self._genre_handler = GenreHandler()
        self._actor_handler = ActorHandler()
        
    
    def start_processing(self) -> None:
        """
        A method that starts processing data by subscribing to Kafka topics.

        Parameters:
            kafka_topics (List[str]): A list of Kafka topics to subscribe to.

        Returns:
            None
        """
        self._kafka_interface.subscribe_to_topics(self.send_to_handler)

    def send_to_handler(self, topic: str, data: dict) -> None:
        """
        Sends the data to the appropriate handler based on the Kafka topic.

        Parameters:
        - topic (str): Kafka topic from which the data is received.
        - data (dict): Raw data received from Kafka message.
        """
        if topic is not None:
            movie: Movie = Movie.from_dict(data)
            self._process_message(movie)
        else:
            raise NoTopicGivenError()
        
    def _process_message(self, movie: Movie):
        """
        Processes each Kafka message by formatting the data and updating the DataFrame.

        Parameters:
        - data (dict): Raw data received from Kafka message.
        """
        if movie.movie_name is not None:
            movie.awards = self._gather_awards(movie.movie_name)
        imdb_id = movie.imdb_id
        movie.genres = self._genre_handler.get_genre(imdb_id)
        movie.lead_actors = self._actor_handler.get_actor(imdb_id)
        
        self._pyspark_interface.edit_row_and_visualize(imdb_id, movie)        
            
    def _gather_awards(self, movie_name: str) -> str:
        """
        Gather awards for a given movie name.

        Parameters:
            movie_name (str): The name of the movie.

        Returns:
            str: A comma-separated string of awards, or None if no awards are found.
        """
        awards = []
        oscar_awards = self._oscar_handler.get_awards(movie_name) or []
        bafta_awards = self._bafta_handler.get_awards(movie_name) or []
        
        awards.extend(oscar_awards + bafta_awards)
        return ', '.join(awards) if awards else None
