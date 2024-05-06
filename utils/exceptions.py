class NoTopicGivenError(ValueError):
    """
    Custom exception raised when no topic is given in the message.
    """
    def __init__(self, message="No topic was given in the message!"):
        self.message = message
        super().__init__(self.message)
        
class NoIMDBInMovieError(ValueError):
    """
    Custom exception raised when no IMDB ID is found in the movie.
    """
    def __init__(self, message="No IMDB ID in the movie!"):
        self.message = message
        super().__init__(self.message)
        
class PotentialSqlInjectionError(ValueError):
    """
    Custom exception raised when a potential SQL injection is detected.
    """
    def __init__(self, message="Potential SQL injection detected!"):
        self.message = message
        super().__init__(self.message)