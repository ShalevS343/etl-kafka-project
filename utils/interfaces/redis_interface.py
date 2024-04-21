import redis
from redis.exceptions import ResponseError
from redisearch import Client

from enum import Enum
import logging
from typing import List

from utils.config import Config
from utils.data_structures.movie import Movie
from utils.exceptions import NoIMDBInMovieError
from utils.schemas import Schemas
from utils.singleton import Singleton

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MovieField(Enum):
    IMDB_ID = 'imdb_id'
    MOVIE_NAME = 'movie_name'
    GENRES = 'genres'
    DIRECTORS = 'directors'
    LEAD_ACTORS = 'lead_actors'
    RATING = 'rating'
    AWARDS = 'awards'
    RELEASE_DATE = 'release_date'


class RedisInterface(Singleton):
    def __init__(self):
        self._redis: redis.StrictRedis = redis.StrictRedis.from_url(Config.REDIS_URI)
        self._redis_index = Config.REDIS_INDEX
        self._index_exists()
        self._client: Client = Client(self._redis_index, conn=self._redis)
        

    def _index_exists(self):
        try:
            # Try to get information about the index
            self._redis.execute_command('FT.INFO', self._redis_index)
        except redis.exceptions.ResponseError:
            self._create_index()

    def _create_index(self):
        self._client: Client = Client(self._redis_index, conn=self._redis)
        self._client.create_index(Schemas.REDIS_SCHEMA)

    def set_value(self, movie: Movie) -> int:
        """
        Set a value in the Redisearch index.

        Parameters:
            key (str): The key of the value to set.
            value (Movie): The value to set.

        Returns:
            int: The number of documents added to the index.
        """
        try:        
            if movie.imdb_id is None:
                raise NoIMDBInMovieError()

            # Format data
            if None in movie.__dict__.values():
                movie = self._format_data(movie)
                
            # Add the document to the Redisearch index
            self._client.add_document(movie.imdb_id, **movie.__dict__)
        except Exception as e:
            print(movie)
            logger.error(e)

    def _format_data(self, movie: Movie) -> Movie:
        """
        Formats the given movie object by replacing any None values in its attributes with the string "None".
        
        Parameters:
            movie (Movie): The movie object to be formatted.
        
        Returns:
            Movie: The formatted movie object.
        """
        movie = Movie.from_dict({k: "None" if v is None else v for k, v in movie.__dict__.items()})
        return movie

    def _decode(self, results) -> List[dict]:
        decoded_movies = []
        for movie in results:
            decoded_movie = {}
            for key, value in movie.items():
                decoded_key = key.decode('utf-8')
                decoded_value = value.decode('utf-8')
                decoded_movie[decoded_key] = decoded_value
            decoded_movies.append(decoded_movie)
        return decoded_movies

    def _parse(self, response: List) -> List[bytes]:

        # Parse the data into a list of dictionaries
        parsed_results: List = []

        # The response is a list of pairs (key, value)
        imdb_ids: List[bytes] = response[1::2]

        for doc_id in imdb_ids:
            # Get the document from the Redis database
            parsed_results.append(self._redis.hgetall(doc_id))

        return parsed_results

    def _movie_search(self, field: MovieField, term: str) -> List:
        try:
            response: List = self._redis.execute_command(
                'FT.SEARCH', self._redis_index, f'@{field.value}:{term}')

            if response[0] == 0:
                return []
            
            # Parse the results parsed results
            parsed_results: List[bytes] = self._parse(response)

            # Return decoded results
            return self._decode(parsed_results)
        except ResponseError as e:
            logger.error(f"Error searching movies: {e}")
            return []

    def get_by_id(self, imdb_id: str):
        return self._movie_search(MovieField.IMDB_ID, f'{imdb_id}')

    def get_by_genre(self, genre):
        return self._movie_search(MovieField.GENRES, f'*{genre}*')

    def get_by_director(self, director):
        return self._movie_search(MovieField.DIRECTORS, f'*{director}*')

    def get_by_year(self, year):
        return self._movie_search(MovieField.RELEASE_DATE, year)

    def get_by_name(self, movie_name):
        return self._movie_search(MovieField.MOVIE_NAME, movie_name)

    def get_by_rating(self, rating):
        return self._movie_search(MovieField.RATING, rating)

    def get_by_actor(self, actor):
        return self._movie_search(MovieField.LEAD_ACTORS, f'*{actor}*')

    def get_by_award(self, award):
        return self._movie_search(MovieField.AWARDS, f'*{award}*')
