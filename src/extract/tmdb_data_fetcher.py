import requests
from src.extract.thread_pool_manager import ThreadPoolManager
from utils.config import Config
from utils.interfaces.redis_interface import redis_interface
from src.extract.data_fetcher import DataFetcher


class TMDBDataFetcher(DataFetcher):
    
    @staticmethod
    def fetch(start_index):
        """
        Runs all of the functions in the class to get all of the data needed from the TMDB API.

        Parameters:
        - start_index: Start index for the current run.

        Returns:
        A dictionary containing all of the needed movie data from this API.
        """
        params = {
            'max_range': Config.PAGE_PER_SCAN,
            'max_workers': Config.MAX_WORKERS,
            'type': 10,
            'start_index': start_index,
            'max_pages': Config.MAX_PAGES
        }
        tmdb_data = ThreadPoolManager.execute_threads(TMDBDataFetcher._fetch_movies, params=params)
        additional_tmdb_data = ThreadPoolManager.execute_threads(TMDBDataFetcher._fetch_data, {**params, 'max_workers': 2 * Config.MAX_WORKERS, 'type': 1, 'data': tmdb_data})
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

        response = requests.get(base_url, headers=Config.TMDB_HEADERS,
                                params={
                                    "page": page, 
                                    "with_original_language": "en", 
                                    "region": "US",
                                    "primary_release_date.lte": "2006-12-31",
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
        response = requests.get(url, headers=Config.TMDB_HEADERS).json()
        if redis_interface.get_by_id(response['imdb_id'])[0] != 0:
            return {}
        return {result[0]: {'imdb_id': response['imdb_id'], 'rating': response['vote_average'][0]}}