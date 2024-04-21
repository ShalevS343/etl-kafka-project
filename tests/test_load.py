import json
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from utils.config import Config
from utils.interfaces.redis_interface import RedisInterface

if __name__ == "__main__":
    Config.validate_config()

    r = RedisInterface()
    k = r.get_by_name("The Term")
    print(json.dumps(k, indent=2))