from abc import ABC, abstractmethod
from typing import List


class AwardsHandler(ABC):
    @abstractmethod
    def get_awards(self, film: str) -> List[str]:
        """
        Retrieves the list of awards for a given film.

        Parameters:
            film (str): The name of the film.

        Returns:
            List[str]: A list of awards for the film.
        """
        pass