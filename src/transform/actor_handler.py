import pandas as pd
from pathlib import Path


class ActorHandler:
    def __init__(self):
        _current_dir = Path(__file__).resolve().parent
        _csv_file_path = _current_dir.parent.parent / "res" / "actors.csv"

        # Read the CSV file using Pandas
        self._df = pd.read_csv(_csv_file_path)

    def get_actor(self, imdb_id: str) -> str:
        """
        Fetches the actor from the DataFrame based on the IMDB ID.

        Parameters:
        - imdb_id (str): The IMDB ID of the movie.

        Returns:
        - actor (str): The actor of the movie.
        """
        actors = self._df[self._df['FilmID'] == imdb_id]['Actor'].tolist()
        if not actors:
            return None
        return ', '.join(actors)
