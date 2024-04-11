import redis
from redis.exceptions import ResponseError
from redisearch import Client, Query, TextField, TagField

from utils.config import Config
from utils.logging import logger

class RedisInterface:
    def __init__(self):
        self._redis = redis.StrictRedis.from_url(Config.REDIS_URI)
        self._index_exists('etl-db')        

    def _index_exists(self, index_name):
        try:
            # Try to get information about the index
            self._redis.execute_command('FT.INFO', index_name)
            self._client = Client(index_name, conn=self._redis)
        except redis.exceptions.ResponseError:
            self._create_index()

    def _create_index(self):
        # Define the schema for the index
        schema = [
            TextField('movie_name', sortable=True),
            TagField('genres'),
            TagField('directors'),
            TagField('lead_actors'),
            TextField('rating', sortable=True),
            TagField('awards'),
            TextField('release_date', sortable=True)
        ]
        self._client = Client('etl-db', conn=self._redis)
        self._client.create_index(schema)

    def set_value(self, key, value):
        """
        Sets a key-value pair in Redis, ensuring that it adheres to the index schema.

        Parameters:
        - key: The key to set.
        - value: The value to set for the key. Should be a dictionary representing the movie data.
        """
        try:
            cleaned_value = {k: v if v is not None else "None" for k, v in value.items()}
            
            # Convert array values to comma-separated strings
            for field, field_value in value.items():
                if isinstance(field_value, list):
                    cleaned_value[field] = ','.join(field_value)

            # Add the document to the Redisearch index
            self._client.add_document(key, **cleaned_value)
        except ResponseError as e:
            logger.error(f"Error adding document to index: {e}")

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

    def _movie_search(self, field, term):
        try:
            if field in ['genres', 'directors', 'lead_actors', 'awards']:
                response = self._redis.execute_command('FT.SEARCH', 'etl-db', f'@{field}:{{*{term}*}}')
            else:
                response = self._redis.execute_command('FT.SEARCH', 'etl-db', f'@{field}:{term}')
            
            # Parse and return the results
            parsed_results = []
            for doc_id in response[1::2]:
                parsed_results.append(self._redis.hgetall(doc_id))
            
            # Return decoded results
            return self._decode(parsed_results)
        except ResponseError as e:
            logger.error(f"Error searching movies: {e}")
            return []
    def get_by_id(self, imdb_id):
        return self._movie_search('imdb_id', imdb_id)

    def get_by_genre(self, genre):
        return self._movie_search('genres', genre)

    def get_by_director(self, director):
        return self._movie_search('directors', director)

    def get_by_year(self, year):
        return self._movie_search('release_date', year)

    def get_by_name(self, movie_name):
        return self._movie_search('movie_name', movie_name)

    def get_by_rating(self, rating):
        return self._movie_search('rating', rating)

    def get_by_actor(self, actor):
        return self._movie_search('lead_actors', actor)

    def get_by_award(self, award):
        return self._movie_search('awards', award)

redis_interface = RedisInterface()
