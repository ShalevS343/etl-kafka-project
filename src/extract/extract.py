import json
import threading
import os
import time

from src.extract.omdb_data_fetcher import OMDBDataFetcher
from src.extract.thread_pool_manager import ThreadPoolManager
from src.extract.tmdb_data_fetcher import TMDBDataFetcher
from utils.config import Config
from utils.interfaces.kafka_interface import kafka_interface
from utils.logging import logger


# TODO: add an abstract class for the data_fetcher
class Extract():
    def __init__(self, conf=300):
        """
        Initializes the Extract class.

        Parameters:
        - interval: The time interval (in seconds) for running the extraction process (default is 300 seconds).
        """
        self._new_movies = {}
        self._conf = conf
        self._stop_event = threading.Event()

    def start(self):
        """
        Starts the background extraction process using a thread pool.
        """
        params = {
            'max_workers': 1,
            'max_range': Config.PAGE_PER_SCAN,
            'type': 1, 
            'start_index': 0,
            'max_pages': Config.MAX_PAGES
        }

        self._stop_event.clear()
        self._thread_pool = ThreadPoolManager.execute_threads(self._run, params)

    def _run(self, params):
        """
        Runs the extraction process in the background.

        The process reads PAGE_PER_SCAN pages every set amount of seconds and stops when reaching MAX_PAGES.
        """
        try:
            while not self._stop_event.is_set():
                # Background code to be executed
                self._gather_movie_data(params)

                params['start_index'] += Config.PAGE_PER_SCAN
                for waiting_time in range(self._conf):
                    logger.info("Next scan for pages %d-%d in %d seconds", 
                                        params['start_index'], 
                                        params['start_index'] + Config.PAGE_PER_SCAN, 
                                        self._conf - waiting_time)
                    time.sleep(1)
                # self.stop()
            
        except KeyboardInterrupt:
            self.stop()


    def stop(self):
        """
        Stops the background extraction thread pool.
        """
        self._stop_event.set()
        # Wait for thread pool to complete
        if hasattr(self, '_thread_pool'):
            self._thread_pool.join()

    def _remove_duplicate_movies(self, existing_data, new_data):
        """
        Removes duplicate movies from the new data.

        Parameters:
        - existing_data: The existing data to compare against.
        - new_data: The new data containing movies to check for duplicates.
        """
        self._new_movies = {title: new_data[title] for title in new_data if title not in existing_data}
        self._new_movies = list(self._new_movies.items())
        logger.info(f'{len(self._new_movies)} New titles added')

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


    def _gather_movie_data(self, params):
        """
        Gathers movie data from TMDB and OMDB APIs, updates JSON files, and send data to Kafka.
        """
        # TMDB API
        tmdb_data = TMDBDataFetcher.fetch(params['start_index'])
        kafka_interface.produce_to_topic('nosaqtgg-tmdb-api', tmdb_data)
        self._update_json_file('tmdb_data.json', tmdb_data)
        
        # OMDB API
        omdb_data = OMDBDataFetcher.fetch(params['start_index'], self._new_movies)
        kafka_interface.produce_to_topic('nosaqtgg-omdb-api', omdb_data)
        self._update_json_file('omdb_data.json', omdb_data)