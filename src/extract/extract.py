import time

from src.extract.omdb_data_fetcher import OMDBDataFetcher
from src.extract.tmdb_data_fetcher import TMDBDataFetcher
from utils.config import Config
from utils.interfaces.kafka_interface import kafka_interface
from utils.logging import logger


class Extract():
    def __init__(self, conf=300):
        """
        Initializes the Extract class.

        Parameters:
        - interval: The time interval (in seconds) for running the extraction process (default is 300 seconds).
        """
        self._new_movies = {}
        self._conf = conf

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

        try:
            while True:
                # Background code to be executed
                self._gather_movie_data(params)

                params['start_index'] += Config.PAGE_PER_SCAN
                for waiting_time in range(self._conf):
                    logger.info("Next scan for pages %d-%d in %d seconds", 
                                        params['start_index'], 
                                        params['start_index'] + Config.PAGE_PER_SCAN, 
                                        self._conf - waiting_time)
                    time.sleep(1)

        except Exception as e:
            logger.error(e)

    def _gather_movie_data(self, params):
        """
        Gathers movie data from TMDB and OMDB APIs, updates JSON files, and send data to Kafka.
        """
        # TMDB API
        tmdb_data = TMDBDataFetcher.fetch(params['start_index'])
        
        # OMDB API
        omdb_data = OMDBDataFetcher.fetch(params['start_index'], tmdb_data)
        
        self._produce(tmdb_data, omdb_data)
        
    def _produce(self, tmdb_data, omdb_data):
        # Zip the two dictionaries together
        zipped_data = zip(tmdb_data.items(), omdb_data.items())

        for (tmdb_key, tmdb_value), (omdb_key, omdb_value) in zipped_data:
            kafka_interface.produce_to_topic('nosaqtgg-tmdb-api', {tmdb_key: tmdb_value})
            kafka_interface.produce_to_topic('nosaqtgg-omdb-api', {omdb_key: omdb_value})
