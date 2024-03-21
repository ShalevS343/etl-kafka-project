from datetime import datetime

class TmdbEntityHandler():

    @staticmethod
    def format_data(data):
        """
        Implementation of the abstract _format_data method.

        Parameters:
        - data (dict): Raw data received from Kafka message.

        Returns:
        - dict: Formatted data with standardized keys.
        """
        formatted_data = {
            'imdb_id': None,
            'movie_name': None,
            'genres': None,
            'directors': None,
            'lead_actors': None,
            'rating': None,
            'awards': None,
            'release_date': None,
        }
        
        data_values = list(data.values())[0]
        if data_values is None:
            raise ValueError("Data values is None")
        formatted_data = {
            **formatted_data, 'imdb_id': data_values.get('imdb_id'),
            'movie_name': list(data.keys())[0], 
            'rating': data_values.get('rating')
        }
        return formatted_data
