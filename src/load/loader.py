from src.load.redis_loader import RedisLoader
from utils.singleton import Singleton
from utils.data_structures.movie import Movie

class Loader(Singleton):
    def __init__(self):
        self._redis_loader = RedisLoader()
        
    def load(self, movie: Movie):
        """
        Load the given movie into the system.

        Parameters:
            movie (Movie): The movie object to be loaded.

        Returns:
            None
        """
        self._redis_loader.load(movie=movie)
