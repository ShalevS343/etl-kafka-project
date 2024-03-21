from datetime import datetime

class OmdbEntityHandler():

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
        formatted_data['imdb_id'] = data_values.get('imdb_id')
        try:
            formatted_data['release_date'] = datetime.strptime(data_values.get('release_date'), "%d-%m-%Y").date()
        except ValueError:
            pass
        except TypeError:
            pass
        if data_values.get('directors'):
            directors = data_values.get('directors')
            formatted_data['directors'] = [directors] if ',' not in directors else directors.split(', ')
        return formatted_data