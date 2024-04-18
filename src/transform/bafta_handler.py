import pandas as pd
from pathlib import Path
from typing import List

from src.transform.awards_handler import AwardsHandler


class BaftaHandler(AwardsHandler):
    def __init__(self):
        _current_dir = Path(__file__).resolve().parent
        _csv_file_path = _current_dir.parent.parent / "res" / "bafta_awards.csv"
        
        # Read the CSV file using Pandas
        self._df = pd.read_csv(_csv_file_path)

    def get_awards(self, film: str) -> List[str]:
        """
        Retrieves the list of awards for a given film from the BaftaHandler's DataFrame.

        Parameters:
            film (str): The name of the film.

        Returns:
            List[str]: A list of awards for the film, or None if no awards are found.
        """
        
        awards: List[str] = self._df[self._df['nominee'] == film]['category'].tolist()
        if not awards:
            return None
        return awards
