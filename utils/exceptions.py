class NoTopicGivenError(ValueError):
    """
    Custom exception raised when no topic is given in the message.
    """
    def __init__(self, message="No topic was given in the message!"):
        self.message = message
        super().__init__(self.message)