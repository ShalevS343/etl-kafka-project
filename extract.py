from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import math
import multiprocessing
import os
import requests
import time

from tqdm import tqdm

from kafka_interface import kafka_interface
from variables import MAX_WORKERS, PAGE_PER_SCAN, TMDB_HEADERS, OMDB_API_KEY, MAX_PAGES

class Extract():
    def __init__(self, interval=300):
        """
        Initializes the Extract class.

        Parameters:
        - interval: The time interval (in seconds) for running the extraction process (default is 300 seconds).
        """
        self._new_movies = {}
        self._start_index = 0
        self._interval = interval
        self._process = None

    def start(self):
        """
        Starts the background extraction process.
        """
        self._process = multiprocessing.Process(target=self._run)
        self._process.start()

    def _run(self):
        """
        Runs the extraction process in the background.

        The process reads PAGE_PER_SCAN pages every interval seconds and stops when reaching MAX_PAGES.
        """
        try:
            while True:
                # Background code to be executed
                self.gather_movie_data()

                self._start_index += PAGE_PER_SCAN
                
                for waiting_time in range(self._interval):
                    time.sleep(1)
                    print(f"\rNext scan for pages {self._start_index}-{self._start_index + PAGE_PER_SCAN} in {self._interval - waiting_time} seconds", end='', flush=True)

        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Stops the background extraction process.
        """
        if self._process is not None and self._process.is_alive():
            self._process.terminate()
            self._process.join()

    def _remove_duplicate_movies(self, existing_data, new_data):
        """
        Removes duplicate movies from the new data.

        Parameters:
        - existing_data: The existing data to compare against.
        - new_data: The new data containing movies to check for duplicates.
        """
        self._new_movies = {title: new_data[title] for title in new_data if title not in existing_data}
        self._new_movies = list(self._new_movies.items())
        if len(self._new_movies) == 0:
            print('No new titles') 
        else:
            print(f'{len(self._new_movies)} New titles added')

    def _update_json_file(self, file_name, new_data):
        """
        Updates a JSON file with new data.

        Parameters:
        - file_name: The name of the JSON file to update.
        - new_data: The new data to add to the JSON file.
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, file_name)

        try:
            with open(file_path, 'r') as json_file:
                existing_data = json.load(json_file)
        except FileNotFoundError:
            existing_data = {}
        
        if file_name == 'tmdb_data.json':
            self._remove_duplicate_movies(existing_data, new_data)
        existing_data.update(new_data)
                        
        with open(file_path, 'w') as json_file:
            json.dump(existing_data, json_file, indent=2)

    def _thread_pool(self, func, params: dict):
        """
        Runs multiple threads using a thread pool.

        Parameters:
        - func: The function to be executed by each thread.
        - params: A dictionary containing parameters for the function and thread pool.

        Returns:
        A dictionary containing the aggregated data from all threads.
        """
        with ThreadPoolExecutor(params['max_workers']) as executor:
            data = {}
            total_iterations = params['max_range'] * params['type']
            local_range = 0

            with tqdm(total=total_iterations, desc=f"Running {func.__name__}") as pbar_outer:
                for range_index in range(self._start_index, min(params['max_range'] + self._start_index, MAX_PAGES), min(params['max_range'] + 1 - local_range, params['type'] + local_range)):
                    local_range = range_index
                    params = {**params, 'range_index': range_index}
                    workers = [executor.submit(func, {**params, 'worker_number': worker_number}) for worker_number in range(params['max_workers'])]

                    for worker in workers:
                        thread_data = worker.result()
                        data.update(thread_data) if isinstance(thread_data, dict) else [data.update(thread) for thread in thread_data]  
                        pbar_outer.update(params['type'])
            return data

    def _get_movie_data(self, params):
        """
        Gets additional movie data from the TMDB API.

        Parameters:
        - params: A dictionary containing parameters for the TMDB API request.

        Returns:
        A dictionary containing additional movie data.
        """
        index = (params['range_index'] - self._start_index) * 20 + params['worker_number']
        if index >= len(params['data']):
            return {}
        result = list(params['data'].items())[index]
        url = f"https://api.themoviedb.org/3/movie/{result[1]['id']}?language=en-US"
        response = requests.get(url, headers=TMDB_HEADERS).json()
        return {result[0]: {'imdb_id': response['imdb_id'], 'rating': response['vote_average']}}

    def _tmdb_fetch_data(self, params):
        """
        Fetches base movie data from the TMDB API.

        Parameters:
        - params: A dictionary containing parameters for the TMDB API request.

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

    def _omdb_fetch_data(self, params):
        """
        Fetches data from the OMDB API.

        Parameters:
        - params: A dictionary containing parameters for the OMDB API request.

        Returns:
        A dictionary containing data from the OMDB API.
        """
        index = (params['range_index'] - self._start_index) * 10 + params['worker_number']
        if index >= len(self._new_movies):
            return {}
        
        current_movie = self._new_movies[index]
        imdb_id = current_movie[1].get('imdb_id')
        if not imdb_id:
            return {current_movie[0]: {'release_date': None, 'directors': None}}
        
        url = 'http://www.omdbapi.com/'

        response = requests.get(url, params={"apikey": OMDB_API_KEY, "i": imdb_id})
        response_json = response.json()
        original_date = datetime.strptime(response_json['Released'], "%d %b %Y") if 'Released' in response_json and response_json['Released'] != 'N/A' else None
        formatted_date = original_date.strftime("%d-%m-%Y") if original_date is not None else original_date
        return {current_movie[0]: {'imdb_id': imdb_id, 'directors': response_json['Director'] if 'Director' in response_json else None, 'release_date': formatted_date}}

    def gather_movie_data(self):
        """
        Gathers movie data from TMDB and OMDB APIs, updates JSON files, and simulates sending data to Kafka.
        """
        # TMDB API
        tmdb_data = self._thread_pool(self._tmdb_fetch_data, {'max_range': PAGE_PER_SCAN, 'max_workers': MAX_WORKERS, 'type': 10})
        additional_tmdb_data = self._thread_pool(self._get_movie_data, {'max_range': PAGE_PER_SCAN, 'max_workers': 2 * MAX_WORKERS, 'type': 1, 'data': tmdb_data})
        tmdb_data = {key: {**tmdb_data[key], 'imdb_id': value['imdb_id'], 'rating': value['rating']} for key, value in additional_tmdb_data.items()}
        
        # kafka_interface.produce_to_topic('nosaqtgg-tmdb-api', tmdb_data)
        self._update_json_file('tmdb_data.json', tmdb_data)
        
        # OMDB API
        if not len(self._new_movies):
            return

        omdb_data = self._thread_pool(self._omdb_fetch_data, {'max_range': math.ceil(len(self._new_movies) / MAX_WORKERS), 'max_workers': MAX_WORKERS, 'type': 1})
        
        # kafka_interface.produce_to_topic('nosaqtgg-omdb-api', omdb_data)
        self._update_json_file('omdb_data.json', omdb_data)

# Run section
extractor = Extract(5)
extractor.start()
