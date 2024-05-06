from abc import ABC, abstractmethod
from typing import Dict

from utils.data_structures.movie import Movie
from utils.data_structures.thread_pool_parameters import Parameters


class DataFetcher(ABC):
    @abstractmethod
    def start(self, start_index: int, new_movies: Dict[str, Movie]) -> Dict[str, Movie]:
        """
        Starts the data fetch process for the API the data will be returned as a Movie Dict object with their imdb_id as key.

        Parameters:
            start_index(int): Start index for the current run.
            new_movies(Dict[str, Movie]): New movies added to the program.

        Returns:
            Dict[str, Movie]: Dictionary of Movie objects and imdb_id as key.
        """

        # Default parameters
        params = Parameters()
        return self._fetch_data(params)

    @abstractmethod
    def _fetch_data(self, params: Parameters, worker_number: int, range_index: int) -> Dict[str, Movie]:
        """
        Fetches data from an API.

        Parameters:
            params(Parameters): A parameter object containing parameters from the Threading Pool.

        Returns:
            List[Movie]: List of Movie objects.
        """
