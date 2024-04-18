import json


class Movie:
    """
    A class that represents the parameters for of the movie.
    """

    def __init__(self, movie_name: str = None, rating: str = None, genres: str = None, lead_actors: str = None,
                 directors: str = None, imdb_id: str = None, awards: str = None, release_date: str = None):
        """
        Initialize the Movie object with the given parameters.

        Parameters:
            movie_name (str): The name of the movie.
            rating (str): The rating of the movie.
            genres (str): The genres of the movie.
            lead_actors (str): The lead actors of the movie.
            directors (str): The directors of the movie.
            imdb_id (str): The IMDB ID of the movie.
            awards (str): The awards of the movie.
            release_date (str): The release date of the movie.
        """
        
        self.imdb_id = imdb_id
        self.movie_name = movie_name
        self.genres = genres
        self.directors = directors
        self.lead_actors = lead_actors        
        self.rating = rating
        self.awards = awards
        self.release_date = release_date
    
    def __str__(self):
        """
        Return a string representation of the Movie object.
        """
        return f"Movie: {self.__dict__}"

    @classmethod
    def from_dict(cls, data: dict) -> "Movie":
        """
        Create a Movie object from a dictionary.

        Parameters:
            data (dict): Dictionary containing movie information.

        Returns:
            Movie: A Movie object initialized from the dictionary.
        """

        if "movie_name" not in data or "rating" not in data or "genres" not in data or "lead_actors" not in data or \
                "directors" not in data or "imdb_id" not in data or "awards" not in data or "release_date" not in data:
            raise ValueError("Invalid movie data")

        return cls(**data)

    def add(self, **kwargs) -> None:
        """
        Add keyword arguments to the parameters.

        Parameters:
            **kwargs: Keyword arguments to add to the parameters.
        """
        self.__dict__.update(kwargs)

    class MovieEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Movie):
                return {
                    'imdb_id': obj.imdb_id,
                    'movie_name': obj.movie_name,
                    'genres': obj.genres,
                    'directors': obj.directors,
                    'lead_actors': obj.lead_actors,
                    'rating': obj.rating,
                    'awards': obj.awards,
                    'release_date': obj.release_date
                }
            return super().default(obj)
