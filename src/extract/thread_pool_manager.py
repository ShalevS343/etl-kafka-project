from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

class ThreadPoolManager:
    @staticmethod
    def execute_threads(func, params):
        """
        Runs multiple threads using a thread pool.

        Parameters:
        - func: The function to be executed by each thread.
        - params: A dictionary containing parameters for the function and thread pool.

        Returns:
        A dictionary containing the aggregated data from all threads.
        """
        
        with ThreadPoolExecutor(params['max_workers']) as executor:
            data = {}
            total_iterations = params['max_range'] * params['type']
            local_range = 0

            with tqdm(total=total_iterations, desc=f"Running {func.__name__}") as pbar_outer:
                for range_index in range(params['start_index'], min(params['max_range'] + params['start_index'], params['max_pages']), min(params['max_range'] + 1 - local_range, params['type'] + local_range)):
                    local_range = range_index
                    params = {**params, 'range_index': range_index}
                    workers = [executor.submit(func, {**params, 'worker_number': worker_number}) for worker_number in range(params['max_workers'])]

                    for worker in workers:
                        thread_data = worker.result()
                        data.update(thread_data) if isinstance(thread_data, dict) else [data.update(thread) for thread in thread_data]  
                        pbar_outer.update(params['type'])
            return data
