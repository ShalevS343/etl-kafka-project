import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from src.transform.transform import Transform
from src.transform.actor_handler import actor_handler
from utils.config import Config

if __name__ == "__main__":
    Config.validate_config()

    t = Transform()
    t.start()
    