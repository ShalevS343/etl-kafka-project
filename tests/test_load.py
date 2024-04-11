import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

import json

from src.load.load import Load
from utils.config import Config
from utils.interfaces.redis_interface import redis_interface

if __name__ == "__main__":
    Config.validate_config()

    # k = redis_interface.get_all()
    k = redis_interface.get_by_rating("1")
    print(json.dumps(k, indent=2))
    
    # [redis_interface.delete_key(i) for i in k] 