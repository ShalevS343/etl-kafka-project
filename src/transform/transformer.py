from src.transform.api_entity_handler import ApiEntityHandler


class Transformer:
    def __init__(self):
        """
        Initializes an instance of Transformer.
        """
        self._api_entity_handler = ApiEntityHandler()

    def transform(self):
        """
        Starts Transformation.
        """
        self._api_entity_handler.start_processing()
