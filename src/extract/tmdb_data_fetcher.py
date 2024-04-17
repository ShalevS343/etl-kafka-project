import requests
from typing import Dict, List, Tuple

from src.extract.data_fetcher import DataFetcher
from src.extract.thread_pool_manager import ThreadPoolManager
from utils.config import Config
from utils.data_structures.movie import Movie
from utils.data_structures.thread_pool_parameters import Parameters
from utils.interfaces.redis_interface import redis_interface


class TMDBDataFetcher(DataFetcher):
    def __init__(self, urls: List[str]):
        self._urls = urls
    def start(self, start_index: int) -> Dict[str, Movie]:
        """
        Starts the TMDB data fetch process.

        Args:
            start_index (int): The index to start fetching data from.

        Returns:
            Dict[str, Movie]: A dictionary containing TMDB data for movies. The key is the IMDB ID and the data is the Movie object.
        This method fetches TMDB data for movies using a thread pool. It first creates fetch parameters with the given start index, max workers, and steps.
        Then it executes threads to fetch base movie data using the _fetch_movies method. The fetched data is used to fetch additional data using the _fetch_data method.
        The additional data is then merged with the base data, including the imdb_id and rating. The merged data is returned as a dictionary.
        """
        
        # Prepare the parameters for fetching the movie pages
        fetch_params: Parameters = Parameters(workers=Config.WORKERS, steps=10, start_index=start_index)
        # Fetch the movies from the TMDB API and check if they exist in the redis database
        tmdb_data: Dict[str, Movie] = ThreadPoolManager.execute_threads(self._fetch_data, params=fetch_params)
        if not len(tmdb_data):
            return tmdb_data
        
        # Prepare the parameters for fetching the rating
        fetch_params.workers = 2 * Config.WORKERS
        fetch_params.steps = 1
        fetch_params.movies = tmdb_data
        
        # Fetch the rating from the TMDB API and merge it
        tmdb_data: Dict[str, Movie] = ThreadPoolManager.execute_threads(self._fetch_rating, params=fetch_params)
        
        return tmdb_data

    
    def _fetch_data(self, params: Parameters) -> Dict[str, Movie]:
        """
        Gets movie data from the TMDB API.

        Parameters:
            params(Parameters): A parameter object containing parameters from the Threading Pool.
        Returns:
            Dict[str, Movie]: A dictionary containing id as key and the movie object as value.
        """
        
        # Calculate the page number for each worker
        page: int = params.range_index + params.worker_number
        if not page:
            return {}

        return self._api_call_page(page)
    
    def _api_call_page(self, page: int) -> Dict[str, Movie]:
        """
        Makes an API call to retrieve a specific page of movie data from the TMDB API.

        Parameters:
            page (int): The page number of the movie data to retrieve.

        Returns:
            Dict[str, Movie]: A dictionary containing the movie data, where the key is the movie ID and the value is the Movie object.
        """
        
        # The url for the api call
        url: str = self._urls[0]
        # Make the api call
        response: requests.Response = requests.get(url, headers=Config.TMDB_HEADERS,
                                params={
                                    "page": page, 
                                    "with_original_language": "en", 
                                    "region": "US",
                                    "primary_release_date.lte": "2010-12-31",
                                })
        movie_data: dict = response.json()
        if "results" not in movie_data:
            return {}
        
        return self._format_data_page(movie_data)
        
    def _format_data_page(self, data: dict) -> Dict[str, Movie]:
        """
        Formats the given data into a dictionary of movies.

        Parameters:
            data (dict): The data to be formatted.

        Returns:
            Dict[str, Movie]: A dictionary containing the formatted movies.
        """
        # Format results
        formatted_data: Dict[str, Movie] = {}
        for movie in data.get('results', []):
            if 'id' in movie and 'title' in movie:
                formatted_data[movie['id']] = Movie(movie_name=movie['title'])
            
        return formatted_data


    def _fetch_rating(self, params: Parameters) -> Dict[str, Movie]:
        """
        Gets movie data from the TMDB API.

        Parameters:
        - params: A dictionary containing parameters from the Threading Pool.

        Returns:
        A dictionary containing additional movie data.
        """
        
        # Calculating the index of the current movie with parameters from the threading pool for each page, each page has 20 movies
        index: int = (params.range_index - params.start_index) * 20 + params.worker_number
        if index >= len(params.movies):
            return {}
        
        
        # Get the movie tuple
        movie: Tuple[str, Movie] = list(params.movies.items())[index]
        
        # Get the movie name and id from the movie tuple
        movie_name: str = movie[1].movie_name
        movie_id: str = movie[0]
        
        return self._api_call_rating(movie_id, movie_name)
        
    def _api_call_rating(self, movie_id: str, movie_name: str) -> Dict[str, Movie]:
        """
        Makes an API call to retrieve movie data from the TMDB API.

        Parameters:
            movie_id: The id of the movie.
            movie_name: The name of the movie.

        Returns:
        Dict[str, Movie]: A dictionary containing additional movie data.
        """
        url:str = f"{self._urls[1]}/{movie_id}?language=en-US"
        response: requests.Response = requests.get(
            url, headers=Config.TMDB_HEADERS)
        response_json: dict = response.json()
        
        if "imdb_id" not in response_json:
            return {}
        
        # Check if the movie exists in the redis database
        if redis_interface.get_by_id(response_json['imdb_id']):
            return {}
        
        return self._format_data_rating(response_json, movie_name)
    
    def _format_data_rating(self, response: dict, movie_name: str) -> Dict[str, Movie]:
        """
        Formats the given data into a dictionary of movies.

        Parameters:
            response: The data to be formatted.
            movie_name: The name of the movie.

        Returns:
        Dict[str, Movie]: A dictionary containing the formatted movies.
        """
        # Format results
        return {response['imdb_id']: Movie(imdb_id=response['imdb_id'], rating=str(response['vote_average'])[0], movie_name=movie_name)}