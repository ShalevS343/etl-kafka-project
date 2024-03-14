from abc import ABC
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, DateType

class EntityHandler(ABC):
    """
    Abstract base class for handling entities.

    This class provides functionality to initialize SparkSession and DataFrame schema.
    """

    def __init__(self, spark_master="local[*]"):
        """
        Initializes the EntityHandler with SparkSession and DataFrame schema.

        Parameters:
        - spark_master (str): Spark master URL. Default is "local[*]" for local execution.
        """
        # Create a SparkSession
        self._spark = SparkSession.builder.master(spark_master).appName("Transform").getOrCreate()

        # Define schema for the DataFrame (modify as needed)
        self._movie_schema = StructType([
            StructField("imdb_id", StringType(), True),
            StructField("movie_name", StringType(), True),
            StructField("genres", ArrayType(StringType()), True),
            StructField("directors", ArrayType(StringType()), True),
            StructField("lead_actors", ArrayType(StringType()), True),
            StructField("rating", FloatType(), True),
            StructField("awards", ArrayType(StringType()), True),
            StructField("release_date", DateType(), True)
        ])

        # Create an empty DataFrame
        self._df = self._spark.createDataFrame([], schema=self._movie_schema)
