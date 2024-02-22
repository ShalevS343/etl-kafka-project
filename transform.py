from datetime import datetime

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, DateType

from kafka_interface import kafka_interface


class Transform:
    def __init__(self, kafka_topics, spark_master="local[*]"):
        """
        Initializes the Transform class with SparkSession and DataFrame schema.

        Parameters:
        - kafka_topics (list): List of Kafka topics to consume data from.
        - spark_master (str): Spark master URL. Default is "local[*]" for local execution.
        """
        # Create a SparkSession
        _spark = SparkSession.builder.master(spark_master).appName("RealTimeTransform").getOrCreate()

        # Define schema for the DataFrame
        _movie_schema = StructType([
            StructField("imdb_id", StringType(), True),
            StructField("movie_name", StringType(), True),
            StructField("genres", ArrayType(StringType()), True),
            StructField("directors", ArrayType(StringType()), True),
            StructField("lead_actors", ArrayType(StringType()), True),
            StructField("rating", FloatType(), True),
            StructField("awards", ArrayType(StringType()), True),
            StructField("release_date", DateType(), True)
        ])

        # Initialize class attributes
        self._kafka_topics = kafka_topics 
        self._spark = _spark
        self._df = self._spark.createDataFrame([], schema=_movie_schema)

    def start(self):
        """
        Starts consuming data from Kafka topics and processes each message.
        """
        kafka_interface.consume_from_topic(self._kafka_topics, self.process_message)
        
    def _format_data(self, topic, data):
        """
        Formats raw data received from Kafka messages based on the topic.

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
            'release_date': None,
        }
        
        if topic == 'nosaqtgg-tmdb-api':
            data_values = list(data.values())[0]
            formatted_data = {**formatted_data, 'imdb_id': data_values['imdb_id'], 'movie_name': list(data.keys())[0], 'rating': data_values['rating']}
        else:
            data = list(data.values())[0]
            formatted_data['imdb_id'] = data['imdb_id']
            try:
                formatted_data['release_date'] = datetime.strptime(data['release_date'], "%d-%m-%Y")
            except ValueError:
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
        # Format the raw data
        data = self._format_data(topic, data)
        
        # Convert imdb_id to string for matching with DataFrame
        imdb_id = str(data['imdb_id'])
        
        # Filter existing rows with the same imdb_id
        existing_row = self._df.filter(self._df.imdb_id == imdb_id)

        if existing_row.count() > 0:
            # Update existing row(s)
            for key, value in data.items():
                if value is not None:
                    existing_row = existing_row.withColumn(key, lit(value))

            # Remove existing rows with the same imdb_id
            self._df = self._df.filter(self._df.imdb_id != imdb_id)
            
            # Union the updated row(s) with the existing DataFrame
            self._df = self._df.union(existing_row)
        else:
            # Create a new Row with values for all schema fields
            new_row = Row(**data)

            # Union with the existing DataFrame
            self._df = self._df.unionByName(self._spark.createDataFrame([new_row], schema=self._df.schema))


# Initialize the Transform class with Kafka configuration and topics
kafka_topics = ["nosaqtgg-tmdb-api", "nosaqtgg-omdb-api"]
transform_instance = Transform(kafka_topics)
transform_instance.start()
