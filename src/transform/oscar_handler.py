from pyspark.sql import SparkSession
from pathlib import Path
from src.transform.awards_handler import AwardsHandler

class OscarHandler(AwardsHandler):
    def __init__(self, spark_master="local[*]"):
        _current_dir = Path(__file__).resolve().parent
        _csv_file_path = str(_current_dir.parent.parent/ "src" / "data_sources" / "oscar_awards.csv")
        
        # Create a SparkSession
        _spark = SparkSession.builder.master(spark_master).appName("oscar_csv").getOrCreate()

        # Read the CSV file using SparkSession
        self._df = _spark.read.option("header", "true").csv(_csv_file_path)
        
    def get_awards(self, film):
        """
        Fetches the awards from the DataFrame based on the Film name.

        Parameters:
        - film (str): The name of the movie.

        Returns:
        - awards (array): the awards of the movie.
        """
        awards = self._df.filter(self._df['film'] == film).select('category').collect()
        if not len(awards):
            return None
        return [row['category'] for row in awards]
        
# Create a singleton instance of Genre Handler
oscar_handler = OscarHandler()
