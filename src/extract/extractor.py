import time
from typing import Dict

from src.extract.data_fetcher import DataFetcher
from utils.config import Config
from utils.data_structures.movie import Movie
from utils.interfaces.kafka_interface import kafka_interface
from utils.logging import logger

import json

class Extractor():
    def __init__(self, data_fetchers: Dict[str, DataFetcher], extractor_interval=300):
        """
        Initializes the Extractor class.

        Parameters:
            data_fetchers (Dict[str, DataFetcher]): A dictionary of data fetchers.
            extractor_interval (int): The interval in seconds between each extraction.
        """
        self._data_fetchers: Dict[str, DataFetcher] = data_fetchers
        self._extractor_interval: int = extractor_interval

    def start(self) -> None:
        """
        Starts the extraction process.
        """
        self._extract()
        
    def _extract(self) -> None:
        """
        Extracts data from the data fetchers and send the data through Kafka.
        """
        
        start_index: int = 0
        try:
            while start_index < Config.MAX_PAGES:
                
                # Get the first fetcher from the dictionary
                first_fetcher: DataFetcher = list(self._data_fetchers.items())[0][1]
                # Get the first topic from the dictionary
                topic: str = list(self._data_fetchers.items())[0][0]
                
                # Fetch new movies using the first fetcher
                new_movie_data: Dict[str, Movie] = first_fetcher.start(start_index)
                # Produce the data to the topic
                kafka_interface.produce_to_topic(f"{Config.CLOUDKARAFKA_USERNAME}-{topic}", new_movie_data)
                
                # For every other fetcher send the new movies to return only the data for the needed movies
                for topic, data_fetcher in list(self._data_fetchers.items())[1:]:
                    fetched_data: Dict[str, Movie] = data_fetcher.start(start_index, new_movie_data)
                    kafka_interface.produce_to_topic(f"{Config.CLOUDKARAFKA_USERNAME}-{topic}", fetched_data)
                
                print(json.dumps(new_movie_data, indent=2, cls=Movie.MovieEncoder))
                
                # Pepare for the next scan
                start_index += Config.PAGE_PER_SCAN
                logger.info("Next scan for pages %d-%d in %d seconds",
                            start_index, start_index + Config.PAGE_PER_SCAN, self._extractor_interval)
                            
                time.sleep(self._extractor_interval)
        except Exception as e:
            logger.error(e)