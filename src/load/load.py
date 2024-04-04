from utils.interfaces.redis_interface import redis_interface
from utils.logging import logger

class Load:
    def __init__(self):
        
        pass
    
    def load(self, data):
        redis_interface.set_value(data.get('imdb_id'), data)
        
        

loader = Load()