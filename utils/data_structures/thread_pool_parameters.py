from typing import Dict

from utils.data_structures.movie import Movie
from utils.config import Config


class Parameters:
    """
    A class that represents the parameters for the thread pool.
    """

    def __init__(self, movies: Dict[str, Movie]=None, worker_number: int=None, max_range: int=Config.PAGE_PER_SCAN,
                 workers=1, max_pages=Config.MAX_PAGES, steps=1, start_index=0, range_index=0):
        """
        Initialize parameters for the thread pool with provided values.
        

        Parameters:
            movies (Dict[str, Movie]): The movies dictionary to pass to the callback.
            max_range (int): The maximum range of pages to fetch.
            workers (int): The number of workers to use for fetching.
            max_pages (int): The maximum number of pages to fetch.
            steps (int): The amount of steps to use for fetching.
            start_index (int): The starting index of which to fetch from.
            range_index (int): The range index for the current range.
            worker_number (int): The worker number for the current worker.
        """
        
        self.movies: Dict[str, Movie] = movies
        self.max_range: int = max_range
        self.workers: int = workers
        self.max_pages: int = max_pages
        self.steps: int = steps
        self.start_index: int = start_index
        self.range_index: int = range_index
        self.worker_number: int = worker_number
    
