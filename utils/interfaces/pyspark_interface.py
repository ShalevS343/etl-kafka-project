from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

import gc

from src.load.loader import Loader
from utils.data_structures.movie import Movie
from utils.schemas import Schemas
from utils.singleton import Singleton

class PysparkInterface(Singleton):

    def __init__(self, spark_master="local[*]", checkpoint_dir="/tmp/spark-checkpoints", num_partitions=15):
        """
        Initializes the EntityHandler with SparkSession and DataFrame schema.

        Parameters:
        - spark_master (str): Spark master URL. Default is "local[*]" for local execution.
        - checkpoint_dir (str): Directory path for storing checkpoints.
        - num_partitions (int): Number of partitions for the DataFrame.
        """
        self._num_partitions = num_partitions
        
        self._spark = SparkSession.builder.master(spark_master).appName("Transform") \
            .config("spark.driver.memory", "4g").config("spark.executor.memory", "8g") \
            .config("spark.sql.shuffle.partitions", str(num_partitions)) \
            .getOrCreate()

        SparkContext.setCheckpointDir(self._spark, checkpoint_dir)

        self._movie_schema = Schemas.PYSPARK_SCHEMA
        self._df = self._spark.createDataFrame([], schema=self._movie_schema).repartition(num_partitions)
        self._df.persist(StorageLevel.DISK_ONLY)
        self._loader = Loader()

    def edit_row_and_visualize(self, imdb_id: str, movie: Movie):
        """
        Updates an existing row or adds a new row in the DataFrame based on the given IMDB ID and Movie object.
        """
        existing_row = self._df.filter(col("imdb_id") == imdb_id)
        
        if existing_row.count() > 0:
            for key, value in movie.__dict__.items():
                if value is not None:
                    existing_row = existing_row.withColumn(key, lit(value))
            existing_row = existing_row.withColumn("touch_counter", col("touch_counter") + 1)
            self._df = self._df.filter(col("imdb_id") != imdb_id).union(existing_row)
            existing_row.unpersist(blocking=True)
        else:
            new_row = self._spark.createDataFrame([movie.__dict__], schema=self._df.schema)
            new_row = new_row.withColumn("touch_counter", lit(1))
            self._df = self._df.unionByName(new_row)
            new_row.unpersist(blocking=True)
        
        self._send_to_loader(imdb_id)

        # Checkpointing the DataFrame to break lineage and reduce memory usage
        self._df = self._df.checkpoint(eager=True)

        # Repartitioning and persisting with DISK_ONLY to optimize memory usage
        self._df = self._df.repartition(100).persist(StorageLevel.DISK_ONLY)

        # Force garbage collection to free up memory
        gc.collect()

        # Show the DataFrame count and contents
        if self._df.count() > 0:
            self._df.show()

    def _send_to_loader(self, imdb_id):
        """
        Sends the data corresponding to the given `imdb_id` to the loader.
        """
        filtered_df = self._df.filter((col("imdb_id") == imdb_id) & (col("touch_counter") == 2))
        
        if not filtered_df.isEmpty():
            row = filtered_df.first().asDict()
            self._df = self._df.filter(col("imdb_id") != imdb_id)

            row.pop('touch_counter')
            movie = Movie.from_dict(row)
            self._loader.load(movie)
        
        filtered_df.unpersist(blocking=True)

        # Checkpointing the DataFrame to break lineage and reduce memory usage
        self._df = self._df.checkpoint(eager=True)

        # Repartitioning and persisting with DISK_ONLY to optimize memory usage
        self._df = self._df.repartition(100).persist(StorageLevel.DISK_ONLY)

        # Force garbage collection to free up memory
        gc.collect()
