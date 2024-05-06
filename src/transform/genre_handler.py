import pandas as pd
from pathlib import Path


class GenreHandler:
    def __init__(self):
        _current_dir = Path(__file__).resolve().parent
        _csv_file_path = _current_dir.parent.parent / "res" / "genres.csv"
        
        # Read the CSV file using Pandas
        self._df = pd.read_csv(_csv_file_path)

    def get_genre(self, imdb_id: str) -> str:
        """
        Get the genre of a movie based on its IMDB ID.

        Parameters:
            imdb_id (str): The IMDB ID of the movie.

        Returns:
            str: The genre of the movie with the specified IMDB ID.
        """
        
        genre_series = self._df[self._df['imdb_id'] == imdb_id]['genre']
        if genre_series.empty:
            return None
        return genre_series.iloc[0]
