from datetime import datetime
import math
import requests

from src.extract.data_fetcher import DataFetcher
from src.extract.thread_pool_manager import ThreadPoolManager
from utils.config import Config
from utils.interfaces.redis_interface import redis_interface


class OMDBDataFetcher(DataFetcher):
    @staticmethod
    def fetch(start_index, new_movies):
        """
        Runs all of the functions in the class to get all of the data needed from the OMDB API.

        Parameters:
        - start_index: Start index for the current run.
        - new_movies: New movies added to the program

        Returns:
        A dictionary containing all of the needed movie data from this API.
        """
        
        if not len(new_movies):
            return {}
        
        params = {
            'max_range': math.ceil(len(new_movies) / Config.MAX_WORKERS),
            'max_workers': Config.MAX_WORKERS,
            'type': 1,
            'new_movies': new_movies,
            'start_index': start_index,
            'max_pages': Config.MAX_PAGES}
        omdb_data = ThreadPoolManager.execute_threads(OMDBDataFetcher._fetch_data, params)
        return omdb_data
    
    @staticmethod
    def _fetch_data(params):
        """
        Fetches data from the OMDB API.

        Parameters:
        - params: A dictionary containing parameters from the Threading Pool.

        Returns:
        A dictionary containing data from the OMDB API.
        """
        
        index = (params['range_index'] - params['start_index']) * 10 + params['worker_number']
        if index >= len(params['new_movies']):
            return {}

        current_movie = list(params['new_movies'].items())[index]
        imdb_id = current_movie[1].get('imdb_id')
        
        if not imdb_id:
            return {current_movie[0]: {'release_date': None, 'directors': None}}

        url = 'http://www.omdbapi.com/'

        response = requests.get(url, params={"apikey": Config.OMDB_API_KEY, "i": imdb_id})
        response_json = response.json()
        original_date = datetime.strptime(response_json['Released'], "%d %b %Y") if 'Released' in response_json and response_json['Released'] != 'N/A' else None
        formatted_date = original_date.strftime("%Y-%m-%d") if original_date is not None else None
        year = formatted_date.split('-')[0] if formatted_date is not None else None
        return {current_movie[0]: {'imdb_id': imdb_id, 'directors': response_json['Director'] if 'Director' in response_json else None, 'release_date': year}}
