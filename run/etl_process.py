import sys
import os
from typing import Dict
import multiprocessing

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from utils.config import Config
from src.extract.extractor import Extractor
from src.extract.tmdb_data_fetcher import TMDBDataFetcher
from src.extract.omdb_data_fetcher import OMDBDataFetcher
from src.extract.data_fetcher import DataFetcher

from src.transform.transformer import Transformer

def extract_process():
    # Extract process
    tmdb = TMDBDataFetcher()
    omdb = OMDBDataFetcher()

    fetchers: Dict[str, DataFetcher] = {'tmdb-api': tmdb, 'omdb-api': omdb}
    e = Extractor(fetchers)
    e.start()

def transform_process():    
    # Transform and load process
    t = Transformer()
    t.transform()

if __name__ == "__main__":
    Config.validate_config()

    # Create separate processes for extract and transform
    extract_proc = multiprocessing.Process(target=extract_process)
    transform_proc = multiprocessing.Process(target=transform_process)

    # Start the processes
    extract_proc.start()
    transform_proc.start()

    # Wait for processes to finish
    extract_proc.join()
    transform_proc.join()
