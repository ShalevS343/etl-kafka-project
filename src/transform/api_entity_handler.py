from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import lit
from utils.interfaces.kafka_interface import kafka_interface
from src.transform.entity_handler import EntityHandler

class ApiEntityHandler(EntityHandler):

    def start_processing(self, kafka_topics):
        """
        Starts consuming data from Kafka topics and processes each message.
        """
        kafka_interface.consume_from_topic(kafka_topics, self.process_message)

    def _format_data(self, topic, data):
        """
        Implementation of the abstract _format_data method.

        Parameters:
        - topic (str): Kafka topic from which the data is received.
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

        if topic == 'nosaqtgg-tmdb-api':
            data_values = list(data.values())[0]
            formatted_data = {**formatted_data, 'imdb_id': data_values['imdb_id'],
                              'movie_name': list(data.keys())[0], 'rating': data_values['rating']}
        else:
            data = list(data.values())[0]
            formatted_data['imdb_id'] = data['imdb_id']
            try:
                formatted_data['release_date'] = datetime.strptime(data['release_date'], "%d-%m-%Y")
            except ValueError:
                pass
            except TypeError:
                pass
            if data['directors']:
                formatted_data['directors'] = [data['directors']] if ',' not in data['directors'] else data['directors'].split(', ')
        return formatted_data

    def process_message(self, topic, data):
        """
        Processes each Kafka message by formatting the data and updating the DataFrame.

        Parameters:
        - topic (str): Kafka topic from which the data is received.
        - data (dict): Raw data received from Kafka message.
        """
        # Format the raw data using ApiEntityHandler's _format_data method
        data = self._format_data(topic, data)

        # Convert imdb_id to string for matching with DataFrame
        imdb_id = str(data['imdb_id'])

        # Filter existing rows with the same imdb_id
        existing_row = self._df.filter(f"imdb_id = '{imdb_id}'")
        if existing_row.count() > 0:
            # Simplify the update using Spark SQL
            for key, value in data.items():
                if value is not None:
                    self._df = self._df.withColumn(key, lit(value))

            # Remove existing rows with the same imdb_id using Spark SQL
            self._df = self._df.filter(f"imdb_id != '{imdb_id}'")

            # Union the updated row(s) with the existing DataFrame
            self._df = self._df.union(existing_row)
        else:
            # Simplify the creation of a new Row
            new_row = Row(**data)

            # Union with the existing DataFrame
            self._df = self._df.unionByName(self._spark.createDataFrame([new_row], schema=self._df.schema))