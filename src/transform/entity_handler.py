from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, DateType
from pyspark.sql import Row
from pyspark.sql.functions import lit

class EntityHandler():

    def __init__(self, spark_master="local[*]", num_partitions=4):
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
        self._df = self._spark.createDataFrame([], schema=self._movie_schema).repartition(num_partitions).persist()
        
    def edit_row(self, imdb_id, data):
        existing_row = self._df.filter(f"imdb_id = '{imdb_id}'")
        if existing_row.count() > 0:
            for key, value in data.items():
                if value is not None:
                    existing_row = existing_row.withColumn(key, lit(value))

            self._df = self._df.filter(f"imdb_id != '{imdb_id}'").union(existing_row)
        else:
            new_row = Row(**data)
            self._df = self._df.unionByName(self._spark.createDataFrame([new_row], schema=self._df.schema))
        
        self._df.show()
           
# Create a signleton instance of the EntityHandler 
entity_handler = EntityHandler()
