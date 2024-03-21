from pyspark.sql import SparkSession
from pathlib import Path


class GenreHandler():
    def __init__(self, spark_master="local[*]"):
        _current_dir = Path(__file__).resolve().parent
        _tsv_file_path = str(_current_dir.parent.parent/ "src" / "data_sources" / "genres.tsv")
        
        # Create a SparkSession
        _spark = SparkSession.builder.master(spark_master).appName("genre_csv").getOrCreate()

        # Read the TSV file using SparkSession
        self._df = _spark.read.option("delimiter", "\t").option("header", "true").csv(_tsv_file_path)
        
    def get_genre(self, imdb_id):
        """
        Fetches the genre from the DataFrame based on the IMDB ID.

        Parameters:
        - imdb_id (str): The IMDB ID of the movie.

        Returns:
        - genre (str): The genre of the movie.
        """
        genres = self._df.filter(self._df['tconst'] == imdb_id).select('genres').collect()
        return genres[0]['genres'].split(',')

        
# Create a singleton instance of Genre Handler
genre_handler = GenreHandler()
