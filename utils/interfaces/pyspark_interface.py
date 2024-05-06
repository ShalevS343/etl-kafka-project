from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from src.load.loader import Loader
from utils.data_structures.movie import Movie
from utils.schemas import Schemas
from utils.singleton import Singleton

class PysparkInterface(Singleton):

    def __init__(self, spark_master="local[*]"):
        """
        Initializes the EntityHandler with SparkSession and DataFrame schema.

        Parameters:
        - spark_master (str): Spark master URL. Default is "local[*]" for local execution.
        """
        # Create a SparkSession with optimized configurations
        self._spark = SparkSession.builder.master(spark_master).appName("Transform") \
            .config("spark.driver.memory", "4g").config("spark.executor.memory", "8g") \
            .getOrCreate()

        # Define schema for the DataFrame (modify as needed)
        self._movie_schema = Schemas.PYSPARK_SCHEMA

        # Create an empty DataFrame
        self._df = self._spark.createDataFrame([], schema=self._movie_schema)
        self._df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        self._df.cache()
        self._loader = Loader()

    def edit_row_and_visualize(self, imdb_id: str, movie: Movie):
        # Update existing row or add new row
        existing_row = self._df.filter(col("imdb_id") == imdb_id)
        if existing_row.count() > 0:
            for key, value in movie.__dict__.items():
                if value is not None:
                    existing_row = existing_row.withColumn(key, lit(value))
            existing_row = existing_row.withColumn("touch_counter", col("touch_counter") + 1)
            self._df = self._df.filter(col("imdb_id") != imdb_id).union(existing_row)
        else:
            new_row = self._spark.createDataFrame([movie.__dict__], schema=self._df.schema)
            new_row = new_row.withColumn("touch_counter", lit(1))
            self._df = self._df.unionByName(new_row)
            
            # Clear cache
            new_row.unpersist(blocking=True)
        # Clear cache
        existing_row.unpersist(blocking=True)
        
        self._send_to_loader(imdb_id)
        
        print(self._df.count())
        # Show the DataFrame
        self._df.show()
        

    def _send_to_loader(self, imdb_id):
        # Filter rows where 'imdb_id' matches imdb_id and 'touch_counter' equals 2
        filtered_df = self._df.filter((col("imdb_id") == imdb_id) & (col("touch_counter") == 2))
        filtered_df.unpersist(blocking=True)
        # Load the filtered DataFrame if it's not empty
        if not filtered_df.isEmpty():
            row = filtered_df.first().asDict()

            # Remove the filtered rows from the DataFrame
            self._df = self._df.filter(col("imdb_id") != imdb_id)

            # Persist the DataFrame in memory for faster access
            self._df.cache()

            # Load the data into Redis
            row.pop('touch_counter')
            movie = Movie.from_dict(row)
            self._loader.load(movie)
