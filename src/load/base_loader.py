from abc import ABC, abstractmethod

from utils.data_structures.movie import Movie

class BaseLoader(ABC):

    @abstractmethod
    def load(self, movie: Movie):
        """
        Load the given movie into the system.

        Parameters:
            movie (Movie): The movie object to be loaded.

        Returns:
            None
        """
        pass