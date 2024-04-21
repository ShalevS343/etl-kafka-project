from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit, when, col

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
        # Create a SparkSession
        self._spark = SparkSession.builder.master(spark_master).appName("Transform") \
            .config("spark.driver.memory", "1g").config("spark.executor.memory", "4g") \
            .getOrCreate()

        # Define schema for the DataFrame (modify as needed)
        self._movie_schema = Schemas.PYSPARK_SCHEMA

        # Create an empty DataFrame
        self._df = self._spark.createDataFrame([], schema=self._movie_schema)
        self._loader = Loader()
        
    def edit_row_and_visualize(self, imdb_id: str, movie: Movie):
                
        # Find a row with the same 'imdb_id'
        existing_row = self._df.filter(f"imdb_id = '{imdb_id}'")
        
        # If there is a row with the same 'imdb_id', update it
        if existing_row.count() > 0:
            for key, value in movie.__dict__.items():
                if value is not None:
                    existing_row = existing_row.withColumn(key, lit(value))

            self._df = self._df.filter(f"imdb_id != '{imdb_id}'").union(existing_row)
        else:
            # If there is no row with the same 'imdb_id', add a new row
            new_row = Row(**{**movie.__dict__, 'touch_counter': 0})
            self._df = self._df.unionByName(self._spark.createDataFrame([new_row], schema=self._df.schema))
        
        # Increment the 'touch_counter' column for rows where 'imdb_id' matches imdb_id
        self._df = self._df.withColumn('touch_counter', 
                                    when(col('imdb_id') == imdb_id, self._df['touch_counter'] + 1)
                                    .otherwise(self._df['touch_counter']))

        # Filter rows where 'imdb_id' matches imdb_id and 'touch_counter' equals 2
        filtered_df = self._df.filter((col('imdb_id') == imdb_id) & (col('touch_counter') == 2))
        row = filtered_df.first()
        
        # Load the filtered DataFrame if it's not empty
        if not filtered_df.isEmpty():
            # Update dataframe to remove row
            self._df = self._df.filter(f"imdb_id != '{imdb_id}'")
            
            # Clear cache
            self._spark.catalog.clearCache()
                        
            # Load the data into Redis
            row = filtered_df.first()
            json_row = row.asDict()
            json_row.pop('touch_counter')
            movie = Movie.from_dict(json_row)
            self._loader.load(movie)
        
        self._df.show()