from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType
from pyspark.sql import Row
from pyspark.sql.functions import lit, when, col
from src.load.load import loader

class EntityHandler():

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
        self._movie_schema = StructType([
            StructField("imdb_id", StringType(), True),
            StructField("movie_name", StringType(), True),
            StructField("genres", ArrayType(StringType()), True),
            StructField("directors", ArrayType(StringType()), True),
            StructField("lead_actors", ArrayType(StringType()), True),
            StructField("rating", StringType(), True),
            StructField("awards", ArrayType(StringType()), True),
            StructField("release_date", StringType(), True),
            StructField("tc", IntegerType(), True)
        ])

        # Create an empty DataFrame
        self._df = self._spark.createDataFrame([], schema=self._movie_schema).persist()
        
    def edit_row(self, imdb_id, data):
        existing_row = self._df.filter(f"imdb_id = '{imdb_id}'")
        if existing_row.count() > 0:
            for key, value in data.items():
                if value is not None:
                    existing_row = existing_row.withColumn(key, lit(value))

            self._df = self._df.filter(f"imdb_id != '{imdb_id}'").union(existing_row)
        else:
            new_row = Row(**{**data, 'tc': 0})
            self._df = self._df.unionByName(self._spark.createDataFrame([new_row], schema=self._df.schema))
        
        # Increment the 'tc' column for rows where 'imdb_id' matches imdb_id
        self._df = self._df.withColumn('tc', 
                                    when(col('imdb_id') == imdb_id, self._df['tc'] + 1)
                                    .otherwise(self._df['tc']))

        # Filter rows where 'imdb_id' matches imdb_id and 'tc' equals 2
        filtered_df = self._df.filter((col('imdb_id') == imdb_id) & (col('tc') == 2))
        row = filtered_df.first()
        
        # Load the filtered DataFrame if it's not empty
        if not filtered_df.isEmpty():
            row = filtered_df.first()
            json_row = row.asDict()
            print(json_row)
            json_row.pop('tc')
            loader.load(json_row)
            self._df = self._df.filter(f"imdb_id != '{imdb_id}'")
        
        self._df.show()
           
# Create a signleton instance of the EntityHandler 
entity_handler = EntityHandler()
