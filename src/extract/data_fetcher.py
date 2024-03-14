from abc import ABC, abstractmethod


class DataFetcher(ABC):
    @abstractmethod
    def fetch(self, start_index, new_movies):
        """
        Runs all of the functions in the class to get all of the data needed from the OMDB API.

        Parameters:
        - params: Start index for the current run.

        Returns:
        A dictionary containing all of the needed movie data from this API.
        """
    
    @abstractmethod
    def _fetch_data(self, params):
        """
        Fetches data from the OMDB API.

        Parameters:
        - params: A dictionary containing parameters from the Threading Pool.

        Returns:
        A dictionary containing data from the OMDB API.
        """