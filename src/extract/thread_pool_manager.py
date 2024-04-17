from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict

from utils.data_structures.movie import Movie
from utils.data_structures.thread_pool_parameters import Parameters

class ThreadPoolManager:
    @staticmethod
    def execute_threads(callback: Callable, params: Parameters) -> Dict[str, Movie]:
        """
        Executes threads in a thread pool.
        
        Parameters:
        - callback (Callable): The function to execute in the thread pool.
        - params (Parameters): The parameters for the thread pool.
        
        Returns:
        - Dict[str, Movie]: A dictionary containing the results of the threads.
        """
        
        with ThreadPoolExecutor(params.workers) as executor:
            data: Dict[str, Movie] = {}
            
            # The loop will iterate from the start_index to the max_range by steps of 
            for range_index in range(params.start_index, params.max_range,
                                    min(params.max_range + 1 - params.range_index, params.steps + params.range_index)):
                params.range_index = range_index
                
                workers = []
                for worker_number in range(params.workers):
                    params.worker_number = worker_number
                    workers.append(executor.submit(callback, params))

                # Gets results from the workers and adds them to the data list
                for worker in workers:
                    worker_result = worker.result()
                    data.update(worker_result) if isinstance(worker_result, dict) else [data.update(result) for result in worker_result]
            return data
