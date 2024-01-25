import requests
import json
from concurrent.futures import ThreadPoolExecutor
import os
from variables import MAX_WORKERS, TOTAL_PAGES, TMDB_HEADERS, RAPID_HEADERS
from tqdm import tqdm

# Class that revolves around every extraction of data in the Project data will be transffered 
# to Kafka Cluster (cloud cluster) for the next step of the proccess
class Extract():
    
    # TODO: Implement realtime features such as running the code every hour or so with 50 other pages
    def __init__(self):
        self.gather_movie_data()
    
    
    # Checks if new movies were found when running the program
    # After checking it will transfer new data into Kafka Producer
    def remove_duplicate_movies(self, existing_data, new_data):
        new_ids = [new_data[title]['id'] for title in new_data if title not in existing_data]
        print('No new titles') if len(new_ids) == 0 else print(f'{len(new_ids)} New titles added')
        # [rapid_api_gather(title, new_data[title]['year']) for title in new_titles]
    
    
    # Temporary function to work with json until Kafka is set up
    # TODO: Add logic to update response_data.json in order to check requests from Rapid API
    def update_json_file(self, file_name, new_data):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, file_name)

        try:
            with open(file_path, 'r') as json_file:
                existing_data = json.load(json_file)
        except FileNotFoundError:
            existing_data = {}
        
        if file_name == 'data.json':
            self.remove_duplicate_movies(existing_data, new_data)
            existing_data.update(new_data)
        else:
            pass
            # {existing_data[key]= value for key, value in new_data.items()}
            # print(items)
                
        with open(file_path, 'w') as json_file:
            json.dump(existing_data, json_file, indent=2)
    
    
    # Generic function to run multiple threads at the same time (also shows progression in terminal)
    def thread_pool(self, func, params: dict):
        with ThreadPoolExecutor(params['max_workers']) as executor:
            # Initialize an empty dictionary to store the data
            data = {}
            total_iterations = params['max_range'] * params['max_workers']

            # Create an outer progress bar for each function call
            with tqdm(total=total_iterations, desc=f"Running {func.__name__}") as pbar_outer:
                for range_index in range(1, params['max_range'] + 1, params['max_workers']):
                    params = {**params, 'range_index': range_index}
                    workers = [executor.submit(func, {**params, 'worker_number': worker_number}) for worker_number in range(params['max_workers'])]

                    # Inner loop: Update progress bar for each worker
                    for worker in workers:
                        try:
                            thread_data = worker.result()
                            data.update(thread_data) if isinstance(thread_data, dict) else [data.update(thread) for thread in thread_data]
                        except Exception as e:
                            print(f"Error in thread: {e}")
                        pbar_outer.update(params['max_workers'])  # Increment the inner progress bar
            return data
    
    # Function to get imdb ids from TMDB API
    def get_imdb_ids(self, params):
        results = []
        for index in range(20):
            result = list(params['data'].items())[params['max_range'] * params['worker_number'] + index]
            url = f"https://api.themoviedb.org/3/movie/{result[1]['id']}/external_ids"
            response = requests.get(url, headers=TMDB_HEADERS).json()
            results.append({result[0]: { 'id':response['imdb_id'] }})
        return results
    
    # Function to fetch data about movies from TMDB API
    def fetch_data(self, params):
        base_url = 'https://api.themoviedb.org/3/discover/movie'
        page = params['range_index'] + params['worker_number']
        response = requests.get(base_url, headers=TMDB_HEADERS,
                                params={
                                    "page": page, 
                                    "with_original_language": "en", 
                                    "region": "US"
                                })
        try:
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            data = response.json()
            return {movie['title']: {'id': movie['id'], 'page': page, 'year': movie['release_date'].split('-')[0]} for movie in data.get('results', [])}
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for page {page}: {http_err}")
        except Exception as e:
            print(f"Page {page} doesn't exist {e}")  # Print the response text for debugging
        return {}
    
    # Function that gathers the data of movies from the TMDB API and updates a json file with the data
    def gather_movie_data(self):
        params = {'max_range': TOTAL_PAGES, 'max_workers': MAX_WORKERS}
        movie_data = self.thread_pool(self.fetch_data, params)
        imdb_data = self.thread_pool(self.get_imdb_ids, {**params, 'data': movie_data})
        movie_data = {key: {**movie_data[key], 'id': value['id']} for key, value in imdb_data.items()}
        self.update_json_file('data.json', movie_data)
        

# Run section
Extract()