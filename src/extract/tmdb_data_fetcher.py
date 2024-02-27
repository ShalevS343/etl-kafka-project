import requests
from utils.extract_utils.tmdb_utils import TMDB_HEADERS
from src.extract.thread_pool_manager import ThreadPoolManager
from utils.extract_utils.general import *

class TMDBDataFetcher:
    
    @staticmethod
    def fetch(start_index):
        """
        Runs all of the functions in the class to get all of the data needed from the TMDB API.

        Parameters:
        - params: Start index for the current run.

        Returns:
        A dictionary containing all of the needed movie data from this API.
        """
        params = {
            'max_range': PAGE_PER_SCAN,
            'max_workers': MAX_WORKERS,
            'type': 10,
            'start_index': start_index,
            'max_pages': MAX_PAGES
        }
        tmdb_data = ThreadPoolManager.execute_threads(TMDBDataFetcher._fetch_movies, params=params)
        additional_tmdb_data = ThreadPoolManager.execute_threads(TMDBDataFetcher._fetch_data, {**params, 'max_workers': 2 * MAX_WORKERS, 'type': 1, 'data': tmdb_data})
        tmdb_data = {key: {**tmdb_data[key], 'imdb_id': value['imdb_id'], 'rating': value['rating']} for key, value in additional_tmdb_data.items()}

        return tmdb_data

    
    @staticmethod
    def _fetch_movies(params):
        """
        Fetches base movie data from the TMDB API.

        Parameters:
        - params: A dictionary containing parameters from the Threading Pool.

        Returns:
        A dictionary containing base movie data.
        """
        
        base_url = 'https://api.themoviedb.org/3/discover/movie'
        page = params['range_index'] + params['worker_number']

        if not page:
            return {}

        response = requests.get(base_url, headers=TMDB_HEADERS,
                                params={
                                    "page": page, 
                                    "with_original_language": "en", 
                                    "region": "US"
                                })
        data = response.json()
        return {movie['title']: {'id': movie['id'], 'page': page} for movie in data.get('results', [])}


    @staticmethod
    def _fetch_data(params):
        """
        Gets movie data from the TMDB API.

        Parameters:
        - params: A dictionary containing parameters from the Threading Pool.

        Returns:
        A dictionary containing additional movie data.
        """
        index = (params['range_index'] - params['start_index']) * 20 + params['worker_number']
        if index >= len(params['data']):
            return {}
        result = list(params['data'].items())[index]
        url = f"https://api.themoviedb.org/3/movie/{result[1]['id']}?language=en-US"
        response = requests.get(url, headers=TMDB_HEADERS).json()
        return {result[0]: {'imdb_id': response['imdb_id'], 'rating': response['vote_average']}}