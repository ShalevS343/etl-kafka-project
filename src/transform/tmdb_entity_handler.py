from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import lit
from src.transform.entity_handler import EntityHandler
import json

class TmdbEntityHandler(EntityHandler):

    def _format_data(self, data):
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
            'release_date': datetime.strptime('01-01-0001', "%d-%m-%Y"),
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

    def process_message(self, data):
        """
        Processes each Kafka message by formatting the data and updating the DataFrame.

        Parameters:
        - data (dict): Raw data received from Kafka message.
        """
        data = self._format_data(data)
        imdb_id = str(data.get('imdb_id'))

        existing_row = self._df.filter(f"imdb_id = '{imdb_id}'")
        if existing_row.count() > 0:
            for key, value in data.items():
                if value is not None:
                    self._df = self._df.withColumn(key, lit(value))

            self._df = self._df.filter(f"imdb_id != '{imdb_id}'")
            self._df = self._df.union(existing_row)
        else:
            new_row = Row(**data)
            self._df = self._df.unionByName(self._spark.createDataFrame([new_row], schema=self._df.schema))