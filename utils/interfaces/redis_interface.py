import redis
from redis.exceptions import ResponseError
from redisearch import Client
from typing import List
from enum import Enum

from utils.config import Config
from utils.data_structures.movie import Movie
from utils.logging import logger
from utils.schemas import Schemas
from utils.singleton import Singleton


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
    def __init__(self, redis_index: str = 'etl-db'):
        self._redis: redis.StrictRedis = redis.StrictRedis.from_url(
            Config.REDIS_URI)
        self._redis_index = redis_index
        self._index_exists(self._redis_index)

    def _index_exists(self, index_name: int):
        try:
            # Try to get information about the index
            self._redis.execute_command('FT.INFO', index_name)
            self._client: Client = Client(index_name, conn=self._redis)
        except redis.exceptions.ResponseError:
            self._create_index()

    def _create_index(self):
        self._client: Client = Client(self._redis_index, conn=self._redis)
        self._client.create_index(Schemas.REDIS_SCHEMA)

    def set_value(self, imdb_id: str, movie: Movie) -> int:
        """
        Set a value in the Redisearch index.

        Parameters:
            key (str): The key of the value to set.
            value (Movie): The value to set.

        Returns:
            int: The number of documents added to the index.
        """
        try:
            cleaned_value = self._sanitize_data(movie)

            # Add the document to the Redisearch index
            self._client.add_document(imdb_id, **cleaned_value)

            return len(cleaned_value)
        except ResponseError:
            return -1

    def _sanitize_data(self, movie):
        cleaned_movie = {
            k: v if v is not None else "None" for k, v in movie.items()}

        # Convert array values to comma-separated strings
        for field, field_value in movie.items():
            if isinstance(field_value, list):
                cleaned_movie[field] = ','.join(field_value)

    def _decode(self, results):
        decoded_movies_list = []
        for movie in results:
            decoded_movie = {}
            for key, value in movie.items():
                decoded_key = key.decode('utf-8')
                decoded_value = value.decode('utf-8')
                decoded_movie[decoded_key] = decoded_value
            decoded_movies_list.append(decoded_movie)
        return decoded_movies_list

    def _parse(self, response: List) -> List[bytes]:

        # Parse the data into a list of dictionaries
        parsed_results: List = []

        # The response is a list of pairs (key, value)
        imdb_ids: List[bytes] = response[1::2]

        for doc_id in imdb_ids:
            # Get the document from the Redis database
            parsed_results.append(self._redis.hgetall(doc_id))

        return parsed_results

    def _movie_search(self, field: MovieField, term: str):
        try:
            response: List = self._redis.execute_command(
                'FT.SEARCH', 'etl-db', f'@{field.value}:{term}')

            # Parse the results parsed results
            parsed_results: List[bytes] = self._parse(response)

            # Return decoded results
            return self._decode(parsed_results)
        except ResponseError as e:
            logger.error(f"Error searching movies: {e}")
            return []

    def get_by_id(self, imdb_id: str):
        return self._movie_search(MovieField.IMDB_ID, imdb_id)

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
