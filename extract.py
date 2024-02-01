import requests
import json
from concurrent.futures import ThreadPoolExecutor
import os
from variables import MAX_WORKERS, TOTAL_PAGES, TMDB_HEADERS, RAPID_HEADERS
from tqdm import tqdm
import math

# Class that revolves around every extraction of data in the Project data will be transffered 
# to Kafka Cluster (cloud cluster) for the next step of the proccess
class Extract():
    
    # TODO: Implement realtime features such as running the code every hour or so with 50 other pages
    def __init__(self):
        self.new_movies = {}
        self.gather_movie_data()
    
    
    # Checks if new movies were found when running the program
    # After checking it will transfer new data into Kafka Producer
    def remove_duplicate_movies(self, existing_data, new_data):
        self.new_movies = {title: new_data[title] for title in new_data if title not in existing_data}
        self.new_movies = list(self.new_movies.items())
        if len(self.new_movies) == 0:
            print('No new titles') 
        else:
            print(f'{len(self.new_movies)} New titles added')
    
    
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
        
        if file_name == 'tmdb_data.json':
            self.remove_duplicate_movies(existing_data, new_data)
        existing_data.update(new_data)
                        
        with open(file_path, 'w') as json_file:
            json.dump(existing_data, json_file, indent=2)
    
    
    # Generic function to run multiple threads at the same time (also shows progression in terminal)
    def thread_pool(self, func, params: dict):
        with ThreadPoolExecutor(params['max_workers']) as executor:
            # Initialize an empty dictionary to store the data
            data = {}
            total_iterations = params['max_range'] * params['type']

            # Create an outer progress bar for each function call
            with tqdm(total=total_iterations, desc=f"Running {func.__name__}") as pbar_outer:
                for range_index in range(0, params['max_range'] + 1, params['type']):                    
                    params = {**params, 'range_index': range_index}
                    workers = [executor.submit(func, {**params, 'worker_number': worker_number}) for worker_number in range(params['max_workers'])]

                    # Inner loop: Update progress bar for each worker
                    for worker in workers:
                        try:
                            thread_data = worker.result()
                            data.update(thread_data) if isinstance(thread_data, dict) else [data.update(thread) for thread in thread_data]
                        except Exception as e:
                            print(f"Error in thread: {e}")
                        pbar_outer.update(params['type'])  # Increment the inner progress bar
            return data
    
    # Function to get imdb ids and rating from TMDB API
    def get_movie_data(self, params):
        print(len(params['data']), params['range_index'] * 20 + params['worker_number'])
        if params['range_index'] * 20 + params['worker_number'] > len(params['data']):
            return {}
        result = list(params['data'].items())[params['range_index'] * 20 + params['worker_number']]
        url = f"https://api.themoviedb.org/3/movie/{result[1]['id']}?language=en-US"
        response = requests.get(url, headers=TMDB_HEADERS).json()
        return {result[0]: { 'imdb_id':response['imdb_id'], 'rating': response['vote_average'] }}

    
    # Function to fetch data about movies from TMDB API
    def tmdb_fetch_data(self, params):
        base_url = 'https://api.themoviedb.org/3/discover/movie'
        page = params['range_index'] + params['worker_number']
        
        if not page:
            return {}
                
        response = requests.get(base_url, headers=TMDB_HEADERS,
                                params={
                                    "page": page, 
                                    "with_original_language": "en", 
                                    "region": "US"
                                })
        try:
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            data = response.json()
            return {movie['title']: {'id': movie['id'], 'page': page} for movie in data.get('results', [])}
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for page {page}: {http_err}")
        except Exception as e:
            print(f"Page {page} doesn't exist {e}")  # Print the response text for debugging
        return {}
    
    
    def rapid_fetch_data(self, params):
        index = params['range_index'] * 10 + params['worker_number']
        current_movie = self.new_movies[index]
        try: 
            imdb_id = current_movie[1].get('imdb_id')

            if not imdb_id:
                return {current_movie[0]: {'release_date': None}}
            
            url = f"https://moviesdatabase.p.rapidapi.com/titles/{imdb_id}"
            query = {"titleType": "movie"}

            response = requests.get(url, headers=RAPID_HEADERS, params=query)
            response_json = response.json()

            if response_json.get('results'):
                release_date = response_json['results'].get('releaseDate')
                return {current_movie[0]: {'release_date': release_date}}
            
            
        
        except Exception as e:
            print(f"Error processing movie: {e}")
        
        return {current_movie[0]: {'release_date': None}}
            
    
    # Function that gathers the data of movies from the TMDB API and updates a json file with the data
    def gather_movie_data(self):
                
        # TMDB API        
        # Gathers base movie data
        tmdb_data = self.thread_pool(self.tmdb_fetch_data, {'max_range': TOTAL_PAGES, 'max_workers': MAX_WORKERS, 'type': 10})
        
        # Gathers additional movie data and appends to base movie data
        additional_tmdb_data = self.thread_pool(self.get_movie_data, {'max_range': TOTAL_PAGES, 'max_workers': 2 * MAX_WORKERS, 'type': 1, 'data': tmdb_data})
        tmdb_data = {key: {**tmdb_data[key], 'imdb_id': value['imdb_id'], 'rating': value['rating'] } for key, value in additional_tmdb_data.items()}
        
        # Write to json file (simulates sending to Producer) also updates self.new_movies dict
        self.update_json_file('tmdb_data.json', tmdb_data)
        
        # RAPID API
        if not len(self.new_movies):
            return

        rapid_data = self.thread_pool(self.rapid_fetch_data, { 'max_range': math.ceil(len(self.new_movies) / MAX_WORKERS), 'max_workers': MAX_WORKERS, 'type': 1 })
        
        self.update_json_file('rapid_data.json', rapid_data)
        

# Run section
Extract()