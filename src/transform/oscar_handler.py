import pandas as pd
from pathlib import Path
from typing import List

from src.transform.awards_handler import AwardsHandler


class OscarHandler(AwardsHandler):
    def __init__(self):
        _current_dir = Path(__file__).resolve().parent
        _csv_file_path = _current_dir.parent.parent / "res" / "oscar_awards.csv"
        
        # Read the CSV file using Pandas
        self._df = pd.read_csv(_csv_file_path)

    def get_awards(self, film: str) -> List[str]:
        """
        Fetches the awards from the DataFrame based on the Film name.

        Parameters:
        - film (str): The name of the movie.

        Returns:
        - awards (list): The awards of the movie.
        """
        awards: List[str] = self._df[self._df['film'] == film]['category'].tolist()
        if not awards:
            return None
        return awards
