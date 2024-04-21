from src.load.base_loader import BaseLoader
from utils.data_structures.movie import Movie
from utils.interfaces.redis_interface import RedisInterface

class RedisLoader(BaseLoader):
    def __init__(self):
        self._redis_interface = RedisInterface() 

    def load(self, movie: Movie) -> None:
        """
        Load the given movie into Redis.

        Parameters:
            movie (Movie): The movie object to be loaded.

        Returns:
            None
        """
        
        self._redis_interface.set_value(movie)