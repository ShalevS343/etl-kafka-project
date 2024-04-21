import sys
import os
from typing import Dict

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from utils.config import Config
from src.extract.extractor import Extractor
from src.extract.tmdb_data_fetcher import TMDBDataFetcher
from src.extract.omdb_data_fetcher import OMDBDataFetcher
from src.extract.data_fetcher import DataFetcher

if __name__ == "__main__":
    Config.validate_config()

    tmdb = TMDBDataFetcher()
    omdb = OMDBDataFetcher()

    fetchers: Dict[str, DataFetcher] = {'tmdb-api': tmdb, 'omdb-api': omdb}
    e = Extractor(fetchers, 5)
    e.start()
