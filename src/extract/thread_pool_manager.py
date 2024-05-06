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

            range_index: int = 0
            steps_to_skip: int = min(
                params.max_range + 1 - range_index, params.steps + range_index)
            # The loop will iterate from the start_index to the max_range by steps of
            for i in range(params.start_index, params.max_range, steps_to_skip):
                range_index: int = i
                steps_to_skip: int = min(
                    params.max_range + 1 - range_index, params.steps + range_index)

                workers = []
                for worker_number in range(params.workers):
                    workers.append(executor.submit(
                        callback, params, worker_number, range_index))

                # Gets results from the workers and adds them to the data list
                for worker in workers:
                    worker_result = worker.result()
                    if isinstance(worker_result, dict):
                        data.update(worker_result)
                    else:
                        for result in worker_result:
                            data.update(result)
            return data
