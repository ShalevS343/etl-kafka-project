from abc import ABC, abstractmethod

class AwardsHandler(ABC):
    @abstractmethod
    def get_awards(self, film):
        """
        Fetches the awards from the DataFrame based on the Film name.

        Parameters:
        - film (str): The name of the movie.

        Returns:
        - awards (array): the awards of the movie.
        """
        pass