from pyspark.sql import SparkSession
from pathlib import Path


class ActorHandler():
    def __init__(self, spark_master="local[*]"):
        _current_dir = Path(__file__).resolve().parent
        _csv_file_path = str(_current_dir.parent.parent/ "src" / "data_sources" / "actors.csv")
        
        # Create a SparkSession
        _spark = SparkSession.builder.master(spark_master).appName("actor_csv").getOrCreate()

        # Read the CSV file using SparkSession
        self._df = _spark.read.option("header", "true").csv(_csv_file_path)
        
    def get_actor(self, imdb_id):
        """
        Fetches the actor from the DataFrame based on the IMDB ID.

        Parameters:
        - imdb_id (str): The IMDB ID of the movie.

        Returns:
        - actor (str): The actor of the movie.
        """
        actors = self._df.filter(self._df['FilmID'] == imdb_id).select('Actor').collect()
        return [row['Actor'] for row in actors]

        
# Create a singleton instance of Genre Handler
actor_handler = ActorHandler()
