import json
class Movie:
    """
    A class that represents the parameters for of the movie.
    """
    
    def __init__(self, movie_name: str=None, rating: str=None, genres: str=None, lead_actors: str=None, \
                directors: str=None, imdb_id: str=None, awards: str=None, release_date: str=None):
        """
        Initializes the Movie object with provided attributes.

        Parameters:
            movie_name (str): The name of the movie.
            rating (str): The rating of the movie.
            genres (str): The genres of the movie.
            lead_actors (str): The lead actors in the movie.
            directors (str): The directors of the movie.
            imdb_id (str): The IMDB ID of the movie.
            awards (str): Any awards received by the movie.
            release_date (str): The year release date of the movie.
        """
        
        self.movie_name = movie_name
        self.rating = rating
        self.genres = genres
        self.lead_actors = lead_actors
        self.directors = directors
        self.imdb_id = imdb_id
        self.awards = awards
        self.release_date = release_date

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
                return obj.__dict__
            return super().default(obj)