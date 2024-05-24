import logging
import time
from typing import Dict, List, Tuple

from src.extract.data_fetcher import DataFetcher
from utils.config import Config
from utils.data_structures.movie import Movie
from utils.interfaces.kafka_interface import KafkaInterface

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Extractor():
    def __init__(self, data_fetchers: Dict[str, DataFetcher], extractor_interval=Config.EXTRACT_INTERVAL):
        """
        Initializes the Extractor class.

        Parameters:
            data_fetchers (Dict[str, DataFetcher]): A dictionary of data fetchers.
            extractor_interval (int): The interval in seconds between each extraction.
        """
        self._data_fetchers: Dict[str, DataFetcher] = data_fetchers
        self._extractor_interval: int = extractor_interval
        self._kafka_interface = KafkaInterface()

    def start(self) -> None:
        """
        Starts the extraction process.
        """
        self._extract()

    def _extract(self) -> None:
        """
        Extracts data from the data fetchers and send the data through Kafka.
        """
        
        logger.info('Extraction Starting...')

        start_index: int = 0
        try:
            while start_index < Config.MAX_PAGES:
                # Get the first fetcher from the dictionary
                first_fetcher: DataFetcher = list(self._data_fetchers.items())[0][1]
                # Get the first topic from the dictionary
                topic: str = list(self._data_fetchers.items())[0][0]

                # Fetch new movies using the first fetcher
                new_movie_data: Dict[str, Movie] = first_fetcher.start(start_index)
                
                topic_data: Dict[str, Dict] = {topic: new_movie_data}
                # For every other fetcher send the new movies to return only the data for the needed movies
                for topic, data_fetcher in list(self._data_fetchers.items())[1:]:
                    fetched_data: Dict[str, Movie] = data_fetcher.start(
                        start_index, new_movie_data)
                    topic_data[topic] = fetched_data
                    
                # Send the data through Kafka
                self._produce(topic_data)

                # Pepare for the next scan
                start_index += Config.PAGE_PER_SCAN
                logger.info("Next scan for pages %d-%d in %d seconds",
                            start_index, start_index + Config.PAGE_PER_SCAN, self._extractor_interval)

                
                time.sleep(self._extractor_interval)
        except Exception as e:
            logger.error(e)

    def _produce(self, topic_data: Dict[str, Dict[str, Movie]]) -> None:
        # Zip the two dictionaries together
        topic_index: int = 0
        topics: List[str] = list(topic_data.keys())
        zipped_data: Dict[str, Dict[str, Movie]] = dict(zip(topics, topic_data.values()))
        while len(zipped_data) > 0:
            topic: str = topics[topic_index]
            
            if topic in zipped_data and len(zipped_data[topic]) > 0:
                message: Tuple [str, Movie] = zipped_data[topic].popitem()
                message_json: Dict[str, Movie] = {message[0]: message[1]} 
                self._kafka_interface.produce_to_topic(f"{Config.CLOUDKARAFKA_USERNAME}-{topic}", message_json)
            else:
                zipped_data.pop(topic)
            topic_index += 1
            topic_index = topic_index % len(topics)