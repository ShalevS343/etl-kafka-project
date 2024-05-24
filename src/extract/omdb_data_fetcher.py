from datetime import datetime
import math
import requests
from typing import Dict

from src.extract.data_fetcher import DataFetcher
from src.extract.thread_pool_manager import ThreadPoolManager
from utils.config import Config
from utils.data_structures.movie import Movie
from utils.data_structures.thread_pool_parameters import Parameters


class OMDBDataFetcher(DataFetcher):
    def __init__(self):
        self._url = Config.OMDB_URL

    def start(self, start_index: int, new_movies: Dict[str, Movie]) -> Dict[str, Movie]:
        """
        A function that starts fetching OMDB data for new movies.

        Parameters:
            start_index (int): The index to start fetching data from.
            new_movies (List[Movie]): A list of new movies to fetch data for.

        Returns:
            List[Movie]: A list of OMDB data for the new movies.
        """
        if not len(new_movies):
            return {}

        omdb_fetch_params = Parameters(max_range=math.ceil(len(new_movies) / Config.WORKERS),
                                       workers=Config.WORKERS, movies=new_movies)
        omdb_data = ThreadPoolManager.execute_threads(
            self._fetch_data, omdb_fetch_params)
        return omdb_data

    def _fetch_data(self, params: Parameters, worker_number: int, range_index: int) -> Dict[str, Movie]:
        """
        Gets the current movie imdb_id and sends it to self._api_call to get the data.

        Parameters:
        - params: A dictionary containing parameters from the Threading Pool.

        Returns:
        A dictionary containing data from the OMDB API.
        """

        # Calculating the index of the current movie with parameters from the threading pool
        index: int = (range_index - params.start_index) * \
            params.workers + worker_number

        # In case the index does not exist in the movie dictionary (a page returned less results than expected)
        if index >= len(params.movies):
            return {}

        # Get the current movie from the movie dictionary
        current_movie: Movie = list(params.movies.items())[index][1]

        # Make the API call to get the data
        return self._api_call(current_movie.imdb_id)

    def _api_call(self, imdb_id: str) -> Dict[str, Movie]:
        """
        Makes an API call to the OMDB API with the given IMDB ID and returns the corresponding movie data.

        Parameters:
            imdb_id (str): A string representing the IMDB ID of the movie.

        Returns:
            Dict[str, Movie]: A dictionary with the IMDB ID and the movie data for the given IMDB ID.
        """

        # Make the API call
        response: requests.Response = requests.get(
            self._url, params={"apikey": Config.OMDB_API_KEY, "i": imdb_id})
        response_json: dict = response.json()

        # Format the data
        return self._format_response(imdb_id, response_json)

    def _format_response(self, imdb_id: str, response_json: dict) -> Dict[str, Movie]:
        """
        Formats the response data from the OMDB API into a dictionary with the IMDB ID as the key.

        Parameters:
            imdb_id (str): The IMDB ID of the movie.
            response_json (dict): The JSON response data from the OMDB API.

        Returns:
            Dict[str, Movie]: A dictionary containing the IMDB ID as the key and a Movie object with formatted data.
        """

        date: datetime = datetime.strptime(
            response_json['Released'], "%d %b %Y") if 'Released' in response_json and response_json['Released'] != 'N/A' else None
        formatted_date: str = date.strftime(
            "%Y-%m-%d") if date is not None else None
        year: str = formatted_date.split(
            '-')[0] if formatted_date is not None else None

        directors: str = response_json['Director'] if 'Director' in response_json else None

        return {imdb_id: Movie(imdb_id=imdb_id, release_date=year, directors=directors)}
