from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

class ThreadPoolManager:
    @staticmethod
    def execute_threads(callback, params: dict):
        """
        Runs multiple threads using a thread pool.

        Parameters:
        - callback: The function to be executed by each thread.
        - params: A dictionary containing parameters for the function and thread pool.

        Returns:
        A dictionary containing the aggregated data from all threads.
        """
        
        with ThreadPoolExecutor(params['max_workers']) as executor:
            data = {}
            total_iterations = params['max_range'] * params['type']
            params['range_index'] = 0

            with tqdm(total=total_iterations, desc=f"Running {callback.__name__}") as pbar_outer:
                for range_index in range(params['start_index'], min(params['max_range'] + params['start_index'], params['max_pages']), min(params['max_range'] + 1 - params['range_index'], params['type'] + params['range_index'])):
                    params['range_index'] = range_index
                    workers = [executor.submit(callback, {**params, 'worker_number': worker_number}) for worker_number in range(params['max_workers'])]

                    for worker in workers:
                        thread_data = worker.result()
                        data.update(thread_data) if isinstance(thread_data, dict) else [data.update(thread) for thread in thread_data]  
                        pbar_outer.update(params['type'])
            return data
