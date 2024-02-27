import json
import multiprocessing
import os
import time

from src.extract.tmdb_data_fetcher import TMDBDataFetcher
from src.extract.omdb_data_fetcher import OMDBDataFetcher
from utils.extract_utils.general import *

import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))
# from src.interfaces.kafka_interface import kafka_interface


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
                self._gather_movie_data()

                self._start_index += PAGE_PER_SCAN
                
                for waiting_time in range(self._interval):
                    time.sleep(1)
                    print(f"\rNext scan for pages {self._start_index}-{self._start_index + PAGE_PER_SCAN} in {self._interval - waiting_time} seconds", end='', flush=True)
                self.stop()
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


    def _gather_movie_data(self):
        """
        Gathers movie data from TMDB and OMDB APIs, updates JSON files, and simulates sending data to Kafka.
        """
        # TMDB API
        tmdb_data = TMDBDataFetcher.fetch(self._start_index)
        
        # kafka_interface.produce_to_topic('nosaqtgg-tmdb-api', tmdb_data)
        self._update_json_file('tmdb_data.json', tmdb_data)
        
        # # OMDB API
        omdb_data = OMDBDataFetcher.fetch(self._start_index, self._new_movies)
        
        # kafka_interface.produce_to_topic('nosaqtgg-omdb-api', omdb_data)
        self._update_json_file('omdb_data.json', omdb_data)